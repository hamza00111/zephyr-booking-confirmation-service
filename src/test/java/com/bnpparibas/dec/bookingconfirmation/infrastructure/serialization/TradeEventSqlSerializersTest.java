package com.bnpparibas.dec.bookingconfirmation.infrastructure.serialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.StringReader;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

/**
 * The SQL-type serializers registered on the trade-event mapper (parity with the publisher
 * service). The Clob failure case matters most: a token must still be written or the generator
 * stream is corrupted.
 */
class TradeEventSqlSerializersTest {

    private final ObjectMapper mapper = TradeEventJacksonConfig.buildTradeEventMapper();

    @Test
    void clob_shouldSerializeAsItsCharacterContent() throws Exception {
        var clob = mock(Clob.class);
        given(clob.getCharacterStream()).willReturn(new StringReader("clob content"));

        assertThat(mapper.writeValueAsString(clob)).isEqualTo("\"clob content\"");
    }

    @Test
    void clob_shouldSerializeAsNull_whenStreamFails() throws Exception {
        var clob = mock(Clob.class);
        given(clob.getCharacterStream()).willThrow(new SQLException("stream unavailable"));

        assertThat(mapper.writeValueAsString(clob)).isEqualTo("null");
    }

    @Test
    void sqlDate_shouldSerializeAsIsoLocalDate() throws Exception {
        assertThat(mapper.writeValueAsString(Date.valueOf(LocalDate.of(2026, 7, 1))))
                .isEqualTo("\"2026-07-01\"");
    }

    @Test
    void sqlTimestamp_shouldSerializeAsIsoLocalDateTime() throws Exception {
        assertThat(mapper.writeValueAsString(Timestamp.valueOf(LocalDateTime.of(2026, 7, 1, 10, 15, 30))))
                .isEqualTo("\"2026-07-01T10:15:30\"");
    }
}
