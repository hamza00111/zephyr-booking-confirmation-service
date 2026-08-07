package com.bnpparibas.dec.bookingconfirmation.domain.model.trade;

import org.jspecify.annotations.Nullable;

/** Consumer-side copy of the shared envelope's audit block. */
public record AuditInfo(@Nullable String initiator, @Nullable String reason, @Nullable String comment) {

    public static AuditInfo nonAudited() {
        return new AuditInfo("N/A", "N/A", "");
    }
}
