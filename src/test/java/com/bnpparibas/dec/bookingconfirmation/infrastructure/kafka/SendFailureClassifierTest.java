package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * The no-data-loss contract for relay send failures: only genuine message-level poison may be parked;
 * everything transient must keep retrying (return false), so a broker outage is never abandoned.
 */
class SendFailureClassifierTest {

    @Test
    void recordTooLarge_isPoison() {
        assertThat(SendFailureClassifier.isPoison(new ExecutionException(new RecordTooLargeException("too big"))))
                .isTrue();
    }

    @Test
    void serialization_isPoison() {
        assertThat(SendFailureClassifier.isPoison(new ExecutionException(new SerializationException("bad bytes"))))
                .isTrue();
    }

    @Test
    void brokerTimeout_isNotPoison_soItKeepsRetrying() {
        assertThat(SendFailureClassifier.isPoison(new ExecutionException(new TimeoutException("broker down"))))
                .isFalse();
    }

    @Test
    void unknownRuntimeFailure_isNotPoison_defaultsToRetry() {
        assertThat(SendFailureClassifier.isPoison(new ExecutionException(new RuntimeException("boom"))))
                .isFalse();
    }
}
