package com.bnpparibas.dec.bookingconfirmation.domain.model;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * A consumed message as stored in (or read from) the inbox.
 *
 * <p>Used both for ingestion (where {@code id} is {@code null} until the row is written) and for
 * draining in the PROCESS stage (where {@code id} is populated). The payload is the raw pivot JSON
 * as received — this is store-and-forward; the pivot is only deserialized when transform logic
 * (enrich/filter) is implemented.
 */
public record InboxMessage(
        @Nullable Long id,
        Region region,
        String idempotencyKey,
        String sourceTopic,
        @Nullable Integer partition,
        @Nullable Long offset,
        @Nullable String messageKey,
        String rawPayload,
        @Nullable String traceId,
        @Nullable String headers) {

    public InboxMessage {
        Objects.requireNonNull(region, "region");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        Objects.requireNonNull(sourceTopic, "sourceTopic");
        Objects.requireNonNull(rawPayload, "rawPayload");
    }
}
