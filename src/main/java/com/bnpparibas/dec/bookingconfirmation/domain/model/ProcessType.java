package com.bnpparibas.dec.bookingconfirmation.domain.model;

/**
 * The three scheduled pipeline stages (the event-driven CONSUME stage is the Kafka listener and is
 * not represented here).
 *
 * <ul>
 *   <li>{@code PROCESS} — drain the inbox, transform, write to the outbox.</li>
 *   <li>{@code RELAY}   — drain the outbox, publish to the region's published topic.</li>
 *   <li>{@code REQUEUE} — promote failed outbox rows back to NEW within the retry budget.</li>
 * </ul>
 */
public enum ProcessType {
    PROCESS,
    RELAY,
    REQUEUE;

    /** Human-readable identifier for a region+stage, e.g. {@code AMER.RELAY}. */
    public String keyFor(final Region region) {
        return region.name() + "." + name();
    }

    public String suffix() {
        return name().toLowerCase();
    }
}
