package com.bnpparibas.dec.bookingconfirmation.infrastructure.serialization;

import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.EventChangeType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeEvent;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Timestamp;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Dedicated mapper for trade-event payloads, mirroring the publisher service's serialization
 * config (mixins for the polymorphic envelope, SQL-type serializers) adapted to Jackson 2.
 *
 * <p>Deliberately NOT {@code @Primary}: a primary ObjectMapper would leak into MVC / actuator /
 * springdoc serialization. Payloads are foreign JSON — this mapper is the only one that touches
 * them, and the application's default mapper never does.
 */
@Configuration
public class TradeEventJacksonConfig {

    public static final String TRADE_EVENT_JSON_MAPPER = "tradeEventJsonMapper";

    @Bean(TRADE_EVENT_JSON_MAPPER)
    public JsonMapper tradeEventJsonMapper() {
        return buildTradeEventMapper();
    }

    /** Static so tests can build the exact production mapper without a Spring context. */
    public static JsonMapper buildTradeEventMapper() {
        final SimpleModule sqlSerializationModule = new SimpleModule("trade-event-sql-types")
                .addSerializer(Clob.class, new ClobDateSerializer())
                .addSerializer(Date.class, new SqlDateSerializer())
                .addSerializer(Timestamp.class, new SqlTimeStampSerializer());
        return JsonMapper.builder()
                .findAndAddModules()
                .enable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
                // Instants as ISO-8601 strings (the wire format), not epoch numbers.
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                // Decimals in the opaque trade payload must not lose precision through a double.
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .addMixIn(TradeEvent.class, TradeEventsMixin.class)
                .addMixIn(EventChangeType.class, TradeEventTypeMixin.class)
                .addModule(sqlSerializationModule)
                .build();
    }
}
