# Zephyr NFR – Descriptions Jira (DECDV-16855)

Pour chaque sous-tâche : ouvre la description, passe l'éditeur sur l'onglet **Texte** (et non Visuel), colle le contenu du bloc, puis enregistre.

## [NFR][TS-01][TC-01] Verify consistency between Booking Confirmation table and Kafka events

```
h3. Objective
Verify consistency between Booking Confirmation table and Kafka events

*Test Suite:* TS-01   *Test Case:* TC-01   *Category:* Data Integrity

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject trades and compare DB records with Kafka messages|Each DB record flagged as sent has the expected Kafka event|
```

## [NFR][TS-01][TC-02] Verify event ordering

```
h3. Objective
Verify event ordering

*Test Suite:* TS-01   *Test Case:* TC-02   *Category:* Data Integrity

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Create, update and delete the same trade|Kafka events are published in the correct order and in the same Kafka partition|
```

## [NFR][TS-02][TC-01] Verify that the system can process the equivalent of one production business day of TDN trades while maintaining acceptable performance and resource utilization

```
h3. Objective
Verify that the system can process the equivalent of one production business day of TDN trades while maintaining acceptable performance and resource utilization

*Test Suite:* TS-02   *Test Case:* TC-01   *Category:* Performance

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Inject a workload equivalent to one production day for TDN accounts|All events are processed successfully|
|2|Measure transactions per second (TPS)|TPS meets or exceeds the target throughput|
|3|Monitor JVM memory consumption|Memory usage remains stable with no excessive garbage collection activity|
|4|Monitor Kafka topic growth and retention|Topic size remains within expected limits|
|5|Analyze database storage consumption (tables and indexes)|Database growth remains within expected capacity limits|
```

## [NFR][TS-02][TC-02] Verify that the system can process three times the daily production volume while maintaining stability, scalability, and data integrity

```
h3. Objective
Verify that the system can process three times the daily production volume while maintaining stability, scalability, and data integrity

*Test Suite:* TS-02   *Test Case:* TC-02   *Category:* Performance

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Inject a workload equivalent to three production days for TDN accounts|All events are processed successfully|
|2|Measure transactions per second (TPS)|Throughput scales appropriately under increased load|
|3|Monitor JVM memory consumption|No memory leaks or out-of-memory errors occur|
|4|Monitor Kafka topic growth and retention|Kafka remains stable and no partition issues occur|
|5|Analyze database storage consumption (tables and indexes)|Database performance remains within acceptable limits|
```

## [NFR][TS-03][TC-01] Verify that a single Zephyr instance can process events for multiple firms while maintaining acceptable performance and resource utilization

```
h3. Objective
Verify that a single Zephyr instance can process events for multiple firms while maintaining acceptable performance and resource utilization

*Test Suite:* TS-03   *Test Case:* TC-01   *Category:* Scalability

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Execute the complete functional test suite for all 4 firms simultaneously|All tests pass successfully with no performance degradation or processing errors|
```

## [NFR][TS-03][TC-02] Verify that multiple Zephyr instances can process workloads in parallel while maintaining data consistency and throughput

```
h3. Objective
Verify that multiple Zephyr instances can process workloads in parallel while maintaining data consistency and throughput

*Test Suite:* TS-03   *Test Case:* TC-02   *Category:* Scalability

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance per firm (4 instances total)|All instances start successfully and connect to Kafka and the database|
|2|Execute the complete functional test suite for all firms|All trades and events are processed successfully without duplication or data inconsistency|
```

## [NFR][TS-04][TC-01] Verify that Zephyr continues processing events when a Kafka broker becomes unavailable

```
h3. Objective
Verify that Zephyr continues processing events when a Kafka broker becomes unavailable

*Test Suite:* TS-04   *Test Case:* TC-01   *Category:* Resiliency

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start Zephyr with a multi-broker Kafka cluster configured|Zephyr starts successfully and connects to all Kafka brokers|
|2|Stop one Kafka broker|Zephyr continues processing events without service interruption or data loss|
```

## [NFR][TS-04][TC-02] Verify that Zephyr can recover from a Kafka outage and correctly process buffered events once Kafka becomes available again

```
h3. Objective
Verify that Zephyr can recover from a Kafka outage and correctly process buffered events once Kafka becomes available again

*Test Suite:* TS-04   *Test Case:* TC-02   *Category:* Resiliency

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start Zephyr while Kafka is unavailable|Zephyr starts successfully and stores incoming events in the database|
|2|Inject trade events into the system|All trade-related events are persisted without loss|
|3|Start the Kafka broker|All pending events are published to Kafka and processing resumes normally|
```

## [NFR][TS-05][TC-01] Verify recovery after database failure

```
h3. Objective
Verify recovery after database failure

*Test Suite:* TS-05   *Test Case:* TC-01   *Category:* Recovery

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start Zephyr with Database failover mode enabled|Zephyr starts successfully and connects to the primary database|
|2|Stop the primary database instance|Zephyr detects the database failure and stops|
|3|Restart Zephyr|Zephyr automatically connects to the secondary database and resumes processing|
```

## [NFR][TS-05][TC-02] Verify recovery after application crash

```
h3. Objective
Verify recovery after application crash

*Test Suite:* TS-05   *Test Case:* TC-02   *Category:* Recovery

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Kill the Zephyr process during processing|Processing stops immediately and any uncommitted transactions are rolled back safely|
|3|Restart Zephyr|Zephyr restarts and resumes without data loss|
```

## [NFR][TS-05][TC-03] Verify that Zephyr can recover successfully when the outage duration exceeds the database UNDO retention period and Flashback query is no longer available

```
h3. Objective
Verify that Zephyr can recover successfully when the outage duration exceeds the database UNDO retention period and Flashback query is no longer available

*Test Suite:* TS-05   *Test Case:* TC-03   *Category:* Recovery

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Kill the Zephyr process and keep it offline for longer than the database UNDO retention period|No processing occurs while the application is offline|
|3|Restart Zephyr|Zephyr starts successfully, detects that Flashback query mode is unavailable, automatically switches to DELTA recovery mode, and resumes processing without data loss|
```

## [NFR][TS-06][TC-01] Verify application logs for successful processing

```
h3. Objective
Verify application logs for successful processing

*Test Suite:* TS-06   *Test Case:* TC-01   *Category:* Monitoring & Observability

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Process valid trades|Logs contain a clear processing status|
```

## [NFR][TS-06][TC-02] Verify error logs for failed events

```
h3. Objective
Verify error logs for failed events

*Test Suite:* TS-06   *Test Case:* TC-02   *Category:* Monitoring & Observability

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject invalid or incomplete trade data|Errors are logged with the trade ID and failure reason|
```

## [NFR][TS-06][TC-03] Verify monitoring metrics

```
h3. Objective
Verify monitoring metrics

*Test Suite:* TS-06   *Test Case:* TC-03   *Category:* Monitoring & Observability

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Run normal and high-volume workloads|Metrics are available (TPS, errors, memory, etc.)|
```
