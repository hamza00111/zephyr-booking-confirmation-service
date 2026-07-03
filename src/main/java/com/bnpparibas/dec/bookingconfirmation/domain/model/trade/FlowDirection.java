package com.bnpparibas.dec.bookingconfirmation.domain.model.trade;

import org.jspecify.annotations.Nullable;

/** Consumer-side copy of the shared framework's flow direction enum. */
public enum FlowDirection {
    INBOUND,
    OUTBOUND;

    public static @Nullable FlowDirection resolve(@Nullable final String externalSystemFlowDir) {
        if (externalSystemFlowDir == null) {
            return null;
        }
        return FlowDirection.valueOf(externalSystemFlowDir);
    }
}
