package com.bnpparibas.dec.bookingconfirmation.common.metric;

/**
 * A module-defined metric identity: the Micrometer meter name ({@link #getKey()}) and its
 * human-readable description. Each module declares its own enum of metrics implementing this
 * interface (the publisher's {@code MetricType}, the processor's
 * {@code BookingConfirmationMetricType}) — the shared {@link MetricsSupport} machinery never needs
 * to know the concrete set.
 *
 * <p>Accessor names match the publisher's Lombok-generated getters so its existing enum only adds
 * {@code implements MetricDefinition}.
 */
public interface MetricDefinition {

    /** Micrometer meter name, e.g. {@code booking.confirmation.publish.success}. */
    String getKey();

    /** Human-readable description shown in the metrics backend. */
    String getDescription();
}
