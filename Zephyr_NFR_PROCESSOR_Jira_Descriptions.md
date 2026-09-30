# Zephyr NFR – PROCESSOR – Descriptions Jira

Coller chaque bloc dans la Description en mode **Text** (pas Visual).

## [PROCESSOR][NFR][TS-01][TC-01] Verify consistency between BOOKING_CONFIRMATION_STAGED_EVENT, BOOKING_CONFIRMATION_PROCESSED_EVENT and published Kafka events

```
h3. Objective
Verify consistency between BOOKING_CONFIRMATION_STAGED_EVENT, BOOKING_CONFIRMATION_PROCESSED_EVENT and published Kafka events

*Test Suite:* TS-01   *Test Case:* TC-01   *Category:* Data Integrity

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject trade events on the internal topic|Every event has a row in {{BOOKING_CONFIRMATION_STAGED_EVENT}}|
|3|Wait for process and relay ticks, then compare {{BOOKING_CONFIRMATION_STAGED_EVENT}}, {{BOOKING_CONFIRMATION_PROCESSED_EVENT}} and the published topic|Every {{PROCESSED}} staged row has one outbound row, and every {{SENT}} outbound row has exactly one published Kafka event|
|4|Check that no row remains in a non-terminal status once the flow is idle|No row left in {{NEW}}, {{PROCESSING}} or {{SEND_IN_PROGRESS}}|
```

## [PROCESSOR][NFR][TS-01][TC-02] Verify per-trade event ordering end to end

```
h3. Objective
Verify per-trade event ordering end to end

*Test Suite:* TS-01   *Test Case:* TC-02   *Category:* Data Integrity

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Create, amend and delete the same trade across several process ticks|Published events follow producer order, share the same message key and land on the same partition|
|3|Check the {{id}} order in {{BOOKING_CONFIRMATION_STAGED_EVENT}} and {{BOOKING_CONFIRMATION_PROCESSED_EVENT}}|{{id}} order matches event order for the trade in both tables|
```

## [PROCESSOR][NFR][TS-01][TC-03] Verify duplicates and redelivery do not create duplicate events

```
h3. Objective
Verify duplicates and redelivery do not create duplicate events

*Test Suite:* TS-01   *Test Case:* TC-03   *Category:* Data Integrity

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject a Kafka batch containing the same event twice, interleaved with other events of the same trade|The duplicate is silently ignored (unique {{region, idempotency_key}}) and arrival order is preserved|
|3|Reset the consumer offset and replay the topic|No new row in {{BOOKING_CONFIRMATION_STAGED_EVENT}}, no duplicate published event|
```

## [PROCESSOR][NFR][TS-02][TC-01] Verify that the processor can handle the equivalent of one production business day of TDN trades while maintaining acceptable performance and resource utilization

```
h3. Objective
Verify that the processor can handle the equivalent of one production business day of TDN trades while maintaining acceptable performance and resource utilization

*Test Suite:* TS-02   *Test Case:* TC-01   *Category:* Performance

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Inject a workload equivalent to one production day for TDN accounts|All events reach a terminal status|
|2|Measure throughput (TPS) and end-to-end latency from ingestion to publication|TPS meets the target and latency stays within the agreed limit|
|3|Monitor the backlog of {{NEW}} rows in both tables|The backlog drains continuously and does not grow over time|
|4|Monitor JVM memory and GC activity|Memory stays stable with no excessive garbage collection|
|5|Analyze database growth of both tables and their indexes|Database growth remains within expected capacity limits|
```

## [PROCESSOR][NFR][TS-02][TC-02] Verify that the processor can handle three times the daily production volume while maintaining stability and data integrity

```
h3. Objective
Verify that the processor can handle three times the daily production volume while maintaining stability and data integrity

*Test Suite:* TS-02   *Test Case:* TC-02   *Category:* Performance

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Inject a workload equivalent to three production days for TDN accounts|All events reach a terminal status|
|2|Measure throughput (TPS) and end-to-end latency|Throughput scales under load, no timeouts on relay sends|
|3|Monitor JVM memory|No memory leaks or out-of-memory errors|
|4|Check per-trade ordering on a sample of trades|No ordering violation under load|
```

## [PROCESSOR][NFR][TS-03][TC-01] Verify that a single processor instance can handle events for multiple firms while maintaining acceptable performance

```
h3. Objective
Verify that a single processor instance can handle events for multiple firms while maintaining acceptable performance

*Test Suite:* TS-03   *Test Case:* TC-01   *Category:* Scalability

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Execute the complete functional test suite for all 4 firms simultaneously|All tests pass with no performance degradation or processing errors|
```

## [PROCESSOR][NFR][TS-03][TC-02] Verify that multiple processor instances can run in parallel without double processing

```
h3. Objective
Verify that multiple processor instances can run in parallel without double processing

*Test Suite:* TS-03   *Test Case:* TC-02   *Category:* Scalability

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance per firm (4 instances total)|Each instance owns only its region partitions|
|2|Execute the complete functional test suite for all firms|No row is claimed twice, no duplicate or out-of-order published event|
|3|Stop one instance during processing|Its partitions are reassigned and processing resumes without loss or duplication|
```

## [PROCESSOR][NFR][TS-04][TC-01] Verify that the relay keeps publishing when a Kafka broker becomes unavailable

```
h3. Objective
Verify that the relay keeps publishing when a Kafka broker becomes unavailable

*Test Suite:* TS-04   *Test Case:* TC-01   *Category:* Resiliency

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start Zephyr with a multi-broker Kafka cluster configured|Zephyr starts and connects to all Kafka brokers|
|2|Stop one Kafka broker during the relay|Events keep being published, no data loss, no duplicate thanks to the idempotent producer|
```

## [PROCESSOR][NFR][TS-04][TC-02] Verify that the processor recovers from a full Kafka outage on the published topic

```
h3. Objective
Verify that the processor recovers from a full Kafka outage on the published topic

*Test Suite:* TS-04   *Test Case:* TC-02   *Category:* Resiliency

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Make the published topic unavailable and inject trade events|Events are processed into {{BOOKING_CONFIRMATION_PROCESSED_EVENT}}, sends fail as {{SEND_FAILURE}}; circuit-breaker rejections do not consume retries|
|3|Restore Kafka|Failed rows are requeued and all pending events are published in order, with no gap and no duplicate|
```

## [PROCESSOR][NFR][TS-05][TC-01] Verify recovery after database failure

```
h3. Objective
Verify recovery after database failure

*Test Suite:* TS-05   *Test Case:* TC-01   *Category:* Recovery

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start Zephyr with database failover mode enabled|Zephyr starts and connects to the primary database|
|2|Stop the primary database instance|Zephyr detects the failure and stops|
|3|Restart Zephyr|Zephyr connects to the secondary database and resumes processing without data loss|
```

## [PROCESSOR][NFR][TS-05][TC-02] Verify recovery after a crash during a process tick

```
h3. Objective
Verify recovery after a crash during a process tick

*Test Suite:* TS-05   *Test Case:* TC-02   *Category:* Recovery

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Kill the Zephyr process while a process tick is running|Rows stay in {{PROCESSING}}; no partial outbound insert (atomic transaction)|
|3|Restart Zephyr and wait for the stale processing delay|Stuck rows are requeued to {{NEW}} and processed once, in order, with no duplicate outbound row|
```

## [PROCESSOR][NFR][TS-05][TC-03] Verify recovery after a crash during a relay tick

```
h3. Objective
Verify recovery after a crash during a relay tick

*Test Suite:* TS-05   *Test Case:* TC-03   *Category:* Recovery

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Kill the Zephyr process while a relay tick is sending|Outbound rows stay in {{SEND_IN_PROGRESS}}|
|3|Restart Zephyr and wait for the stale send delay|Rows are requeued and re-sent in order; events already sent before the crash are deduplicated downstream by idempotency key|
```

## [PROCESSOR][NFR][TS-06][TC-01] Verify application logs for successful processing

```
h3. Objective
Verify application logs for successful processing

*Test Suite:* TS-06   *Test Case:* TC-01   *Category:* Monitoring & Observability

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Process valid trades|Logs show each stage (ingest, process, relay) with trade key and status|
```

## [PROCESSOR][NFR][TS-06][TC-02] Verify error logs for failed events

```
h3. Objective
Verify error logs for failed events

*Test Suite:* TS-06   *Test Case:* TC-02   *Category:* Monitoring & Observability

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject invalid or incomplete trade data|Errors are logged with trade key, stage and failure reason ({{INVALID}}, {{PROCESS_FAILURE}}, {{SEND_FAILURE}})|
```

## [PROCESSOR][NFR][TS-06][TC-03] Verify monitoring metrics and alerting

```
h3. Objective
Verify monitoring metrics and alerting

*Test Suite:* TS-06   *Test Case:* TC-03   *Category:* Monitoring & Observability

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Run normal and high-volume workloads|Metrics are available: TPS, latency, backlog per status, retries|
|3|Force a row into {{RETRY_EXHAUSTED}}|An alert is raised for manual action|
```

## [PROCESSOR][NFR][TS-07][TC-01] Verify create, amend, amend in the same drain collapse into a single create

```
h3. Objective
Verify create, amend, amend in the same drain collapse into a single create

*Test Suite:* TS-07   *Test Case:* TC-01   *Category:* Ordering

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject C1, A2, A3 for the same trade within one process tick|One {{TRADE_CREATED}} is published carrying the A3 payload; the other rows are {{AGGREGATED}}|
```

## [PROCESSOR][NFR][TS-07][TC-02] Verify create, amend, amend split across drains

```
h3. Objective
Verify create, amend, amend split across drains

*Test Suite:* TS-07   *Test Case:* TC-02   *Category:* Ordering

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject C1 and A2, wait for the process tick, then inject A3|{{TRADE_CREATED}} (A2 payload) then {{TRADE_AMENDED}} (A3) are published in this order|
```

## [PROCESSOR][NFR][TS-07][TC-03] Verify create and delete in the same drain net out

```
h3. Objective
Verify create and delete in the same drain net out

*Test Suite:* TS-07   *Test Case:* TC-03   *Category:* Ordering

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject C1, A2, D3 for the same trade within one process tick|Nothing is published; all rows are {{AGGREGATED}}|
```

## [PROCESSOR][NFR][TS-07][TC-04] Verify create and delete split across drains produce a real bust

```
h3. Objective
Verify create and delete split across drains produce a real bust

*Test Suite:* TS-07   *Test Case:* TC-04   *Category:* Ordering

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject C1 and A2, wait for the process tick, then inject D3|{{TRADE_CREATED}} then {{TRADE_DELETED}} are published in this order|
```

## [PROCESSOR][NFR][TS-07][TC-05] Verify an amend is promoted to create when the original create was filtered

```
h3. Objective
Verify an amend is promoted to create when the original create was filtered

*Test Suite:* TS-07   *Test Case:* TC-05   *Category:* Ordering

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject a C1 matching the filter chain, then A2 for the same trade|C1 is {{FILTERED}}; A2 is published as {{TRADE_CREATED}}, never as an orphan amend|
```

## [PROCESSOR][NFR][TS-07][TC-06] Verify only the first amend is promoted after a filtered create

```
h3. Objective
Verify only the first amend is promoted after a filtered create

*Test Suite:* TS-07   *Test Case:* TC-06   *Category:* Ordering

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject a filtered C1, then A2; after the process tick inject A3|A2 is published as {{TRADE_CREATED}}, A3 stays a {{TRADE_AMENDED}} (no second create)|
```

## [PROCESSOR][NFR][TS-07][TC-07] Verify a delete is dropped when the create was filtered

```
h3. Objective
Verify a delete is dropped when the create was filtered

*Test Suite:* TS-07   *Test Case:* TC-07   *Category:* Ordering

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject a filtered C1, then D2 for the same trade|Nothing is published: downstream never saw the trade, so there is nothing to bust|
```

## [PROCESSOR][NFR][TS-07][TC-08] Verify promotion when both the create and the first amend are filtered

```
h3. Objective
Verify promotion when both the create and the first amend are filtered

*Test Suite:* TS-07   *Test Case:* TC-08   *Category:* Ordering

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject a filtered C1, a filtered A2, then A3|One {{TRADE_CREATED}} is published with the A3 payload|
```

## [PROCESSOR][NFR][TS-07][TC-09] Verify keyless events are never blocked and never block

```
h3. Objective
Verify keyless events are never blocked and never block

*Test Suite:* TS-07   *Test Case:* TC-09   *Category:* Ordering

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject events with a null message key, mixed with keyed events|Each keyless event is published as-is; keyed trades are processed normally, with no cross-correlation|
```

## [PROCESSOR][NFR][TS-08][TC-01] Verify an unreadable event does not block later events of the same trade

```
h3. Objective
Verify an unreadable event does not block later events of the same trade

*Test Suite:* TS-08   *Test Case:* TC-01   *Category:* Failure Handling

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject an event with an unreadable event type, then valid events for the same trade|The first row is {{INVALID}} with an error message; later events are processed and published normally|
```

## [PROCESSOR][NFR][TS-08][TC-02] Verify an ingestion failure on a create blocks the trade

```
h3. Objective
Verify an ingestion failure on a create blocks the trade

*Test Suite:* TS-08   *Test Case:* TC-02   *Category:* Failure Handling

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Inject a create whose mapping or insert fails, then an amend for the same trade|The create is parked as {{INGEST_FAILURE}} after retries; the amend stays {{NEW}} and is not published|
|3|Fix and replay the create|Create and amend are published in order|
```

## [PROCESSOR][NFR][TS-08][TC-03] Verify a process failure on a create holds later events of the same trade

```
h3. Objective
Verify a process failure on a create holds later events of the same trade

*Test Suite:* TS-08   *Test Case:* TC-03   *Category:* Failure Handling

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Make the transform fail for C1, then inject A2 and A3 for the same trade|C1 is {{PROCESS_FAILURE}}; A2 and A3 stay {{NEW}} and nothing is published for the trade|
|3|Check another trade processed at the same time|Other trades are not impacted|
|4|Fix and retry C1 manually|The trade's events are published in order|
```

## [PROCESSOR][NFR][TS-08][TC-04] Verify a process failure on an amend does not block later events

```
h3. Objective
Verify a process failure on an amend does not block later events

*Test Suite:* TS-08   *Test Case:* TC-04   *Category:* Failure Handling

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Publish C1, then make the transform fail for A2 and inject A3|A2 is {{PROCESS_FAILURE}}; A3 is processed and published|
```

## [PROCESSOR][NFR][TS-08][TC-05] Verify a delete does not release a trade blocked by a failed create (known gap)

```
h3. Objective
Verify a delete does not release a trade blocked by a failed create (known gap)

*Test Suite:* TS-08   *Test Case:* TC-05   *Category:* Failure Handling

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Make C1 fail at process stage, then inject D2 for the same trade|The trade stays blocked: the supersede-before-delete step is not scheduled yet and manual clearing is required|
```

## [PROCESSOR][NFR][TS-08][TC-06] Verify a send failure on an earlier event holds back later events of the same trade

```
h3. Objective
Verify a send failure on an earlier event holds back later events of the same trade

*Test Suite:* TS-08   *Test Case:* TC-06   *Category:* Failure Handling

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Make the send of the first outbound event of a trade fail while later events are pending|The first row is {{SEND_FAILURE}}; later same-key rows are not published|
|3|Let the requeue tick run with Kafka available|All events of the trade are published in order, with no gap|
```

## [PROCESSOR][NFR][TS-08][TC-07] Verify circuit-breaker rejections do not consume retries

```
h3. Objective
Verify circuit-breaker rejections do not consume retries

*Test Suite:* TS-08   *Test Case:* TC-07   *Category:* Failure Handling

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Open the Kafka circuit breaker and let relay ticks run|Rows go to {{SEND_FAILURE}} without incrementing {{retry_count}}|
|3|Close the circuit breaker|Rows are requeued and published in order|
```

## [PROCESSOR][NFR][TS-08][TC-08] Verify retry exhaustion stalls the trade rather than reordering it

```
h3. Objective
Verify retry exhaustion stalls the trade rather than reordering it

*Test Suite:* TS-08   *Test Case:* TC-08   *Category:* Failure Handling

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Make sends fail for one trade more than {{BC_REQUEUE_MAX_RETRIES}} times|The row becomes {{RETRY_EXHAUSTED}}; later events of the trade are held and an alert is raised|
|3|Check other trades|Other trades keep flowing|
|4|Manually re-drive the row|The held events are published in order|
```

## [PROCESSOR][NFR][TS-08][TC-09] Verify a stale requeued row is superseded by a fresher sibling

```
h3. Objective
Verify a stale requeued row is superseded by a fresher sibling

*Test Suite:* TS-08   *Test Case:* TC-09   *Category:* Failure Handling

h3. Steps / Expected Results
||#||Action||Expected Result||
|1|Start one Zephyr instance configured for 4 firms (GSS, CFL, KOP and SGP)|The instance starts successfully and all firms are loaded|
|2|Leave an earlier event stuck in {{PROCESSING}} while a later event of the same trade is processed|The later event reaches {{PROCESSED}}|
|3|Wait for the stale processing delay and the next process tick|The stuck row is requeued then marked {{SUPERSEDED}}; the stale event is never published|
```

