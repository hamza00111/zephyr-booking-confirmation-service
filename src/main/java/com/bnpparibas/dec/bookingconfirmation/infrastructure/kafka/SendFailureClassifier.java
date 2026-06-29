package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * Classifies a relay send failure as <em>poison</em> (a message-level defect that retrying cannot
 * fix) or <em>infrastructure</em> (a transient outage that will succeed once the broker recovers).
 *
 * <p>This is the crux of the no-data-loss guarantee: infrastructure failures are retried indefinitely
 * (never abandoned because Kafka was down), while only genuine poison is parked for inspection. When
 * in doubt we treat a failure as infrastructure (retry, don't drop) — the "age of oldest unsent row"
 * alert surfaces anything that is stuck retrying so nothing is lost silently either way.
 */
public final class SendFailureClassifier {

    private SendFailureClassifier() {}

    /** @return {@code true} if the cause is a permanent, message-level defect that retrying cannot fix. */
    public static boolean isPoison(final Throwable failure) {
        Throwable cause = failure;
        while (cause != null) {
            if (cause instanceof RecordTooLargeException
                    || cause instanceof SerializationException
                    || cause instanceof UnknownTopicOrPartitionException) {
                return true;
            }
            // Circuit open / interrupted / generic execution wrappers are transient — keep retrying.
            if (cause instanceof CallNotPermittedException || cause instanceof InterruptedException) {
                return false;
            }
            cause = (cause instanceof ExecutionException) ? cause.getCause() : cause.getCause();
        }
        return false;
    }
}
