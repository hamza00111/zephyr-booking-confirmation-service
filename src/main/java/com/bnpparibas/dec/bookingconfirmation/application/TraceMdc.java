package com.bnpparibas.dec.bookingconfirmation.application;

import org.jspecify.annotations.Nullable;
import org.slf4j.MDC;

/**
 * MDC scoping for the inbound {@code traceId} — a correlation id the upstream publisher stamps into
 * the event body ({@code $.traceId}), not a transport header.
 *
 * <p>The pipeline is decoupled (consume -> inbox -> scheduled PROCESS -> outbox -> scheduled RELAY),
 * so trace context cannot ride the thread/async boundaries automatically. The traceId is therefore
 * carried as data on the inbox/outbox rows and re-established in the MDC at each stage with a
 * try-with-resources scope, keeping every log line for one trade correlated:
 *
 * <pre>{@code
 * try (var ignored = TraceMdc.scope(message.traceId())) {
 *     // logs here carry traceId in the MDC
 * }
 * }</pre>
 */
public final class TraceMdc {

    public static final String TRACE_ID = "traceId";

    private TraceMdc() {}

    /** Opens an MDC scope for {@code traceId}; closing it clears the key. A null id becomes empty. */
    public static MDC.MDCCloseable scope(@Nullable final String traceId) {
        return MDC.putCloseable(TRACE_ID, traceId != null ? traceId : "");
    }
}
