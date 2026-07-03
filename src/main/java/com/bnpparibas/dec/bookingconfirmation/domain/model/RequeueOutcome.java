package com.bnpparibas.dec.bookingconfirmation.domain.model;

/**
 * Result of one requeue pass: {@code promoted} rows went back to {@code NEW} for another attempt;
 * {@code exhausted} rows ran out of retry budget and were parked terminally
 * ({@code RETRY_EXHAUSTED} / {@code INVALID}). Reported separately so exhaustion is alertable.
 */
public record RequeueOutcome(int promoted, int exhausted) {

    public static final RequeueOutcome NONE = new RequeueOutcome(0, 0);

    public int total() {
        return promoted + exhausted;
    }
}
