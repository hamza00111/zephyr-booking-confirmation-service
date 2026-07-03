package com.bnpparibas.dec.bookingconfirmation.infrastructure.serialization;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.sql.Timestamp;

/**
 * Serializes {@link Timestamp} as ISO-8601 local date-time (parity with the publisher service's
 * serializer, adapted to the Jackson 2 API).
 */
public class SqlTimeStampSerializer extends JsonSerializer<Timestamp> {

    @Override
    public void serialize(final Timestamp value, final JsonGenerator gen, final SerializerProvider provider)
            throws IOException {
        gen.writeString(value.toLocalDateTime().toString());
    }
}
