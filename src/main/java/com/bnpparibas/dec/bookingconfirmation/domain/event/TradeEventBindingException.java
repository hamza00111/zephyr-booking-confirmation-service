package com.bnpparibas.dec.bookingconfirmation.domain.event;

/**
 * A payload parsed as JSON but could not be bound to (or written from) the typed trade-event
 * envelope. Thrown inside the PROCESS transform block, so the affected aggregation group lands in
 * the {@code PROCESS_FAILURE} → requeue → {@code INVALID} lifecycle; the dedicated type keeps a
 * straight-to-INVALID fast path possible later without inspecting messages.
 */
public class TradeEventBindingException extends RuntimeException {

    public TradeEventBindingException(final String message) {
        super(message);
    }

    public TradeEventBindingException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
