package com.bnpparibas.dec.bookingconfirmation.loadtest;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * High-throughput trade-event producer for aggregation load testing — NOT a test; a {@code main}
 * you run against the local stack while the service is up:
 *
 * <pre>
 * mvn -q test-compile org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
 *     -Dexec.mainClass=com.bnpparibas.dec.bookingconfirmation.loadtest.TradeEventLoadProducer \
 *     -Dexec.classpathScope=test \
 *     -Dexec.args="--trades 100 --events-per-trade 5"
 * </pre>
 *
 * <p>Sends {@code trades × eventsPerTrade} events (default 100 × 5 = 500) to the internal topic,
 * shaped like the real upstream publisher's messages: message key {@code <firm>_<pivotId.id>}
 * (e.g. {@code KOP_444071454_1}), a unique {@code idempotency-key} header per event, and the full
 * UBIX payload envelope. Amendments bump {@code tradeUpdateDateTime}/{@code notional} so
 * latest-payload-wins is observable on the published events.
 *
 * <p>Args (all optional): {@code --bootstrap localhost:9092}, {@code --topic
 * local.booking.trade.internal.amer}, {@code --trades 100}, {@code --events-per-trade 5},
 * {@code --delete-last} (append a trailing TradeDeleted per trade), {@code --rate N} (events/s,
 * 0 = full speed), {@code --base-trade-id 444071454}, {@code --firm KOP}, {@code --hub US},
 * {@code --source-id KOP}, {@code --payload-file event.json}.
 *
 * <p>{@code --payload-file} makes generated events immune to model drift: dump ONE real event
 * from AKHQ/the Confluent panel into a file and pass it here. The file is used as the template
 * for every message; only identity/variance fields are overridden per message (eventType,
 * eventId, traceId, pivotId.id, external trade-id references, occurredAt/recordedAt,
 * payload.tradeUpdateDateTime, payload.notional) and each override is skipped when the template
 * lacks that node — whatever the consuming model requires, the template already satisfies.
 *
 * <p>Aggregation math (also printed after the run): the PROCESS stage groups per message key
 * <em>within one drained batch</em> (batch-size 200, tick 3s). Created+Deleted → nothing; Created
 * present → one TradeCreated with the latest payload; Deleted alone → one TradeDeleted; otherwise
 * one TradeAmended. Expected published count ≈ one per trade (+1/+2 when a drain boundary splits
 * a trade's run).
 */
public final class TradeEventLoadProducer {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private String bootstrap = "localhost:9092";
    private String topic = "local.booking.trade.internal.amer";
    private int trades = 100;
    private int eventsPerTrade = 5;
    private boolean deleteLast = false;
    private double rate = 0;
    private long baseTradeId = 444071454L;
    private String firm = "KOP";
    private String hub = "US";
    private String sourceId = "KOP";
    private String payloadFile = null;
    private ObjectNode payloadTemplate = null;

    public static void main(final String[] args) throws Exception {
        final TradeEventLoadProducer loadProducer = new TradeEventLoadProducer();
        loadProducer.parse(args);
        loadProducer.run();
    }

    private void parse(final String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--bootstrap" -> bootstrap = args[++i];
                case "--topic" -> topic = args[++i];
                case "--trades" -> trades = Integer.parseInt(args[++i]);
                case "--events-per-trade" -> eventsPerTrade = Integer.parseInt(args[++i]);
                case "--delete-last" -> deleteLast = true;
                case "--rate" -> rate = Double.parseDouble(args[++i]);
                case "--base-trade-id" -> baseTradeId = Long.parseLong(args[++i]);
                case "--firm" -> firm = args[++i];
                case "--hub" -> hub = args[++i];
                case "--source-id" -> sourceId = args[++i];
                case "--payload-file" -> payloadFile = args[++i];
                default -> throw new IllegalArgumentException("Unknown arg: " + args[i]);
            }
        }
    }

    private void run() throws InterruptedException, java.io.IOException {
        if (payloadFile != null) {
            payloadTemplate = (ObjectNode) MAPPER.readTree(java.nio.file.Files.readString(
                    java.nio.file.Path.of(payloadFile), StandardCharsets.UTF_8));
            System.out.println("Using payload template: " + payloadFile);
        }
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        final List<String> perTrade = eventTypesPerTrade();
        final long sleepMs = rate > 0 ? Math.round(1000.0 / rate) : 0;
        int created = 0;
        int amended = 0;
        int deleted = 0;
        int sequence = 0;
        final long started = System.nanoTime();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < trades; i++) {
                final String tradeId = (baseTradeId + i) + "_1";
                final String messageKey = firm + "_" + tradeId;
                final Instant now = Instant.now();
                for (int amendmentIndex = 0; amendmentIndex < perTrade.size(); amendmentIndex++) {
                    final String eventType = perTrade.get(amendmentIndex);
                    sequence++;
                    final ProducerRecord<String, String> record = new ProducerRecord<>(
                            topic, null, messageKey, event(eventType, tradeId, amendmentIndex, now));
                    header(record, "idempotency-key",
                            hub + ":" + sourceId + ":" + firm + ":" + tradeId + ":" + sequence);
                    header(record, "source-id", sourceId);
                    header(record, "firm", firm);
                    header(record, "hub", hub);
                    header(record, "booking-confirmation-id", String.valueOf(sequence));
                    header(record, "correlationId", "00-" + UUID.randomUUID() + "-loadtest");
                    producer.send(record);
                    switch (eventType) {
                        case "TradeCreated" -> created++;
                        case "TradeAmended" -> amended++;
                        default -> deleted++;
                    }
                    if (sequence % 100 == 0) {
                        System.out.println("  ... " + sequence + " events sent");
                    }
                    if (sleepMs > 0) {
                        Thread.sleep(sleepMs);
                    }
                }
            }
            producer.flush();
        }
        summarize(created, amended, deleted, sequence, (System.nanoTime() - started) / 1_000_000_000.0);
    }

    private List<String> eventTypesPerTrade() {
        final java.util.ArrayList<String> types = new java.util.ArrayList<>();
        types.add("TradeCreated");
        for (int i = 1; i < eventsPerTrade; i++) {
            types.add("TradeAmended");
        }
        if (deleteLast) {
            types.add("TradeDeleted");
        }
        return types;
    }

    private static void header(final ProducerRecord<String, String> record, final String key, final String value) {
        record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    /** One full envelope; amendments bump tradeUpdateDateTime and notional. */
    private String event(final String eventType, final String tradeId, final int amendmentIndex, final Instant now) {
        if (payloadTemplate != null) {
            return eventFromTemplate(eventType, tradeId, amendmentIndex, now);
        }
        return builtInEvent(eventType, tradeId, amendmentIndex, now);
    }

    /**
     * Deep-copies the real-event template and overrides only identity/variance fields — every
     * override is conditional on the node existing, so the template's shape is never distorted.
     */
    private String eventFromTemplate(
            final String eventType, final String tradeId, final int amendmentIndex, final Instant now) {
        final ObjectNode root = payloadTemplate.deepCopy();
        root.put("eventType", eventType);
        if (root.has("eventId")) {
            root.put("eventId", UUID.randomUUID().toString());
        }
        if (root.has("traceId")) {
            root.put("traceId", UUID.randomUUID().toString());
        }
        if (root.path("pivotId").isObject()) {
            ((ObjectNode) root.get("pivotId")).put("id", tradeId);
        }
        if (root.has("occurredAt")) {
            root.put("occurredAt", now.toString());
        }
        if (root.has("recordedAt")) {
            root.put("recordedAt", now.toString());
        }
        if (root.path("payload").isObject()) {
            final ObjectNode payload = (ObjectNode) root.get("payload");
            if (payload.path("externalSystemTradeId").isObject()) {
                ((ObjectNode) payload.get("externalSystemTradeId")).put("id", tradeId);
            }
            if (payload.path("references").isObject()) {
                ((ObjectNode) payload.get("references")).put("externalSystemTradeId", tradeId);
            }
            if (payload.has("tradeUpdateDateTime")) {
                payload.put("tradeUpdateDateTime", LocalDateTime.ofInstant(
                        now.plusSeconds(amendmentIndex), ZoneOffset.UTC).withNano(0).toString());
            }
            if (payload.has("notional")) {
                payload.put("notional", 1_000_000 + amendmentIndex);
            }
        }
        return root.toString();
    }

    private String builtInEvent(
            final String eventType, final String tradeId, final int amendmentIndex, final Instant now) {
        final LocalDateTime updateTime =
                LocalDateTime.ofInstant(now.plusSeconds(amendmentIndex), ZoneOffset.UTC).withNano(0);
        final LocalDate today = LocalDate.ofInstant(now, ZoneOffset.UTC);

        final ObjectNode root = MAPPER.createObjectNode();
        root.put("version", "1.0");
        root.put("eventType", eventType);
        root.put("eventId", UUID.randomUUID().toString());
        root.putObject("pivotId").put("id", tradeId);
        root.put("traceId", UUID.randomUUID().toString());
        root.put("externalSystem", "UBIX");
        root.put("flowDirection", "INBOUND");
        root.put("hub", hub);
        root.put("occurredAt", now.toString());
        root.put("recordedAt", now.toString());
        final ObjectNode audit = root.putObject("auditInfo");
        audit.put("initiator", "Booking confirmation publisher");
        audit.put("reason", "Automated booking confirmation publishing workflow");
        audit.put("comment", "Zephyr booking confirmation publisher service stream");

        final ObjectNode payload = root.putObject("payload");
        payload.putObject("externalSystemTradeId").put("id", tradeId);
        final ObjectNode externalSystem = payload.putObject("externalSystem");
        externalSystem.put("name", "UBIX");
        externalSystem.put("description", "Ubix back-office");
        final ObjectNode references = payload.putObject("references");
        references.put("externalSystemTradeId", tradeId);
        references.putNull("originalTradeId");
        references.putNull("secondaryTradeId");
        references.putNull("previousTradeId");
        references.putNull("universalTradeId");
        references.putNull("basketId");
        references.put("orderId", "G182559711");
        references.putNull("reportTrackingNumber");
        payload.put("tradeUpdateDateTime", updateTime.toString());
        // Bumped per amendment so "latest payload wins" is visible downstream.
        payload.put("notional", 1_000_000 + amendmentIndex);
        payload.putNull("positionId");
        payload.put("direction", "SELL");
        payload.putObject("quantity").put("value", 1);
        payload.putObject("price").put("value", 100000);
        final ObjectNode parties = payload.putObject("parties");
        parties.putNull("clearingHouse");
        party(parties, "exchange", "XSIM");
        party(parties, "operatingFirm", "SG S007");
        party(parties, "counterParty", "SGP C");
        party(parties, "executingFirm", "MS");
        parties.putNull("giveUpFirm");
        party(parties, "sendingFirm", "MS");
        parties.putNull("executingBroker");
        parties.putNull("trader");
        party(parties, "accountOwner", "D31500");
        payload.putObject("counterpartyAccount").put("code", "SGP C");
        final ObjectNode lifeCycle = payload.putObject("lifeCycle");
        lifeCycle.put("tradeDate", today.toString());
        lifeCycle.put("executionTime", updateTime.toString());
        lifeCycle.put("clearingDate", today.toString());
        lifeCycle.putNull("marketDateTime");
        // Required non-null by the processor's TradeLifeCycle invariants — must live INSIDE lifeCycle.
        lifeCycle.put("matchingStatus", "MATCHED");
        lifeCycle.put("clearingStatus", "CLEARED");
        final ObjectNode tradeSubType = payload.putObject("tradeSubType");
        tradeSubType.putNull("code");
        tradeSubType.putNull("name");
        final ArrayNode fees = payload.putArray("fees");
        for (final String feeType : List.of("EXCHANGE_FEE", "EXECUTION_FEE", "CLEARING_FEE", "NFA_FEE")) {
            final ObjectNode fee = fees.addObject();
            fee.put("type", feeType);
            fee.put("amount", 0);
            fee.put("tax", 0);
            fee.putNull("currency");
        }
        payload.putNull("sessionId");
        payload.putNull("lastCapacity");
        payload.putNull("tradingVenue");
        final ObjectNode venueType = payload.putObject("venueType");
        venueType.put("code", "N");
        venueType.putNull("name");
        final ObjectNode executionSource = payload.putObject("executionSource");
        executionSource.putNull("code");
        executionSource.putNull("name");
        payload.putNull("priceType");
        payload.putNull("allocationAccount");
        payload.putNull("positionAccount");
        return root.toString();
    }

    private static void party(final ObjectNode parties, final String role, final String name) {
        final ObjectNode party = parties.putObject(role);
        party.put("name", name);
        party.putNull("lei");
    }

    private void summarize(
            final int created, final int amended, final int deleted, final int total, final double seconds) {
        System.out.println();
        System.out.printf("Sent %d events in %.1fs (%.0f/s) to %s%n", total, seconds, total / Math.max(seconds, 0.001), topic);
        System.out.printf("  %d TradeCreated, %d TradeAmended, %d TradeDeleted across %d trade key(s) (%s_%d_1 ...)%n",
                created, amended, deleted, trades, firm, baseTradeId);
        System.out.println();
        final int perTradeCount = eventsPerTrade + (deleteLast ? 1 : 0);
        if (deleteLast) {
            System.out.println("Expected after aggregation: 0 published events — every trade's group contains");
            System.out.println("Created+Deleted, which nets out. Inbox: all " + total + " rows AGGREGATED.");
        } else if (trades == 1) {
            System.out.println("Expected after aggregation: ONE published event per PROCESS drain that saw the key");
            System.out.println("(batch-size 200, tick 3s) — roughly ceil(" + total + "/200) at full speed, more if");
            System.out.println("produced slower than the tick. The first is TradeCreated, the rest TradeAmended.");
        } else {
            System.out.println("Expected after aggregation: " + trades + " published events (one TradeCreated per");
            System.out.println("trade, carrying the LAST amendment's payload — notional " + (1_000_000 + perTradeCount - 1) + ").");
            System.out.println("Inbox: " + trades + " PROCESSED + " + (total - trades) + " AGGREGATED. A drain boundary");
            System.out.println("splitting a trade's run adds one extra event for that trade (+1/+2 typical worst case).");
        }
        System.out.println();
        System.out.println("Verify with:");
        System.out.println("  SELECT PROCESSING_STATUS, COUNT(*) FROM BOOKING_CONFIRMATION_INBOX GROUP BY PROCESSING_STATUS;");
        System.out.println("  SELECT PROCESSING_STATUS, COUNT(*) FROM BOOKING_CONFIRMATION_OUTBOX GROUP BY PROCESSING_STATUS;");
    }
}
