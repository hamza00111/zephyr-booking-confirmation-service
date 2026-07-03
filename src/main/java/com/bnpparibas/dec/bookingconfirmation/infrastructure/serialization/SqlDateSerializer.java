package com.bnpparibas.dec.bookingconfirmation.infrastructure.serialization;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.sql.Date;

/**
 * Serializes {@link java.sql.Date} as ISO-8601 local date (parity with the publisher service's
 * serializer, adapted to the Jackson 2 API).
 */
public class SqlDateSerializer extends JsonSerializer<Date> {

    @Override
    public void serialize(final Date value, final JsonGenerator gen, final SerializerProvider provider)
            throws IOException {
        gen.writeString(value.toLocalDate().toString());
    }
}
