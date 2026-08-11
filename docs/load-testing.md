# Load testing & profiling

Shared IntelliJ run configurations live in `.run/` and appear automatically in the
Run dropdown:

| Config | What it does |
|---|---|
| **App (profiling)** | Starts the service (`local` profile) with GC logs, JFR recording, and heap-dump-on-OOM, all written under `target/` |
| **Load 2k burst** | 2,000 distinct trades × 1 event — throughput test, no aggregation collapsing |
| **Load aggregation 100x5** | 100 trades × 5 events — exercises latest-payload-wins aggregation |

The producer configs run `TradeEventLoadProducer` on the test classpath. If IntelliJ
flags the module, pick `zephyr-booking-confirmation-service.test` (or the plain module
name on older module layouts) in the `-cp` dropdown.

Re-running a load config with the same `--base-trade-id` re-sends the same
idempotency keys — all rows dedupe (that is the duplicate-handling test). Pass a new
`--base-trade-id` for a fresh batch.

## CLI equivalents

Load producer (no IDE):

```bash
mvn -q test-compile org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
    -Dexec.mainClass=com.bnpparibas.dec.bookingconfirmation.loadtest.TradeEventLoadProducer \
    -Dexec.classpathScope=test \
    -Dexec.args="--trades 2000 --events-per-trade 1"
```

App with the same profiling flags:

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=local -Dspring-boot.run.jvmArguments="\
  -Xms1g -Xmx1g \
  -Xlog:gc*,gc+heap=info,gc+age=debug,safepoint:file=target/gc-%t.log:time,uptime,level,tags:filecount=5,filesize=20m \
  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=target/ \
  -XX:StartFlightRecording=maxage=30m,filename=target/run.jfr,settings=profile"
```

## While the app runs (`jcmd` ships with the JDK)

```bash
jcmd <pid> GC.heap_dump target/heap-during-load.hprof   # heap dump now (analyze in Eclipse MAT / IntelliJ)
jcmd <pid> GC.class_histogram | head -30                # quick top-of-heap, no dump needed
jcmd <pid> Thread.print > target/threads.txt            # where is every thread stuck
jcmd <pid> JFR.dump filename=target/snapshot.jfr        # cut the rolling JFR window to a file
jstat -gcutil <pid> 1000                                # live GC utilization, 1 line/s
```

Open `.jfr` files in IntelliJ (drag into the editor) or JDK Mission Control; GC logs in
GCViewer/gceasy.io; `.hprof` in Eclipse MAT (Leak Suspects report, then dominator tree).

## Measuring ingestion throughput at the sink

```sql
SELECT COUNT(*), MIN(CREATED_ON), MAX(CREATED_ON)
FROM BOOKING_CONFIRMATION_INBOX
WHERE CREATED_ON > :run_start;
```

`count / (max - min)` is the real consumer rate, independent of any Kafka-UI lag
display quirks (transactional producers inflate lag with control-marker offsets).
The load producer prints the expected inbox/outbox end state and these queries
after every run.
