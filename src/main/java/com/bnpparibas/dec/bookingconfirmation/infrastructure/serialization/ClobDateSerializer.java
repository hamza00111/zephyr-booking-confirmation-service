package com.bnpparibas.dec.bookingconfirmation.infrastructure.serialization;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

/**
 * Streams a {@link Clob} into a JSON string (parity with the publisher service's serializer,
 * adapted to the Jackson 2 API on this classpath). On failure a JSON null is written — Jackson 2
 * requires a token to be emitted or the output stream is corrupted.
 */
@Slf4j
public class ClobDateSerializer extends JsonSerializer<Clob> {

    @Override
    public void serialize(final Clob value, final JsonGenerator gen, final SerializerProvider provider)
            throws IOException {
        try (Reader reader = value.getCharacterStream()) {
            final StringBuilder builder = new StringBuilder();
            final char[] buffer = new char[8192];
            int read;
            while ((read = reader.read(buffer)) != -1) {
                builder.append(buffer, 0, read);
            }
            gen.writeString(builder.toString());
        } catch (final SQLException | IOException e) {
            log.warn("Failed to serialize clob caused by {}", e.getMessage(), e);
            gen.writeNull();
        }
    }
}
