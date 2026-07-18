package com.bnpparibas.dec.bookingconfirmation.common.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Module-agnostic Micrometer support, extracted from the publisher's {@code MetricsSupport} so both
 * modules (publisher and processor) share one metric-building idiom. Destined for the shared
 * common module — keep this package free of module-specific types.
 *
 * <p>Metric identities come from each module's own {@link MetricDefinition} enum; tag conventions
 * (the publisher's source/firm/hub, the processor's region/stage) stay module-side, applied through
 * the fluent builders. Each module exposes this as a bean (it is deliberately not a Spring
 * component — the common module stays framework-free).
 *
 * <p>Registration is idempotent: builders resolve to the registry's existing meter when the same
 * name/tags combination is registered again, so builders can be used at call sites, not only at
 * initialization.
 */
public class MetricsSupport {

    private final MeterRegistry registry;

    public MetricsSupport(final MeterRegistry registry) {
        this.registry = registry;
    }

    public CounterBuilder counter(final MetricDefinition metric) {
        return counter(metric.getKey(), metric.getDescription());
    }

    public CounterBuilder counter(final String name, final String description) {
        return new CounterBuilder(registry, name, description);
    }

    public TimerBuilder timer(final MetricDefinition metric) {
        return timer(metric.getKey(), metric.getDescription());
    }

    public TimerBuilder timer(final String name, final String description) {
        return new TimerBuilder(registry, name, description);
    }

    public void gauge(final MetricDefinition metric, final Supplier<? extends Number> supplier, final Tag... tags) {
        Gauge.builder(metric.getKey(), supplier::get)
                .description(metric.getDescription())
                .tags(List.of(tags))
                .register(registry);
    }

    public void gauge(final String name, final Supplier<? extends Number> supplier, final Tag... tags) {
        Gauge.builder(name, supplier::get).tags(List.of(tags)).register(registry);
    }

    /**
     * Registers one gauge per constant of {@code statusType} (tagged {@code statusTag} = constant
     * name plus the fixed tags), each backed by an {@link AtomicLong} the caller updates via
     * {@link StatusGauges#refresh}. Generic form of the publisher's per-source status gauge block;
     * the caller caches one instance per tag combination (e.g. per source).
     */
    public <E extends Enum<E>> StatusGauges<E> statusGauges(
            final MetricDefinition metric, final Class<E> statusType, final String statusTag, final Tag... fixedTags) {
        final Map<E, AtomicLong> holders = new EnumMap<>(statusType);
        for (final E status : statusType.getEnumConstants()) {
            final AtomicLong holder = new AtomicLong(0L);
            holders.put(status, holder);
            Gauge.builder(metric.getKey(), holder, AtomicLong::get)
                    .description(metric.getDescription())
                    .tags(List.of(fixedTags))
                    .tag(statusTag, status.name())
                    .register(registry);
        }
        return new StatusGauges<>(holders);
    }

    /** Live status-count holders behind an already-registered gauge set. */
    public static final class StatusGauges<E extends Enum<E>> {

        private final Map<E, AtomicLong> holders;

        private StatusGauges(final Map<E, AtomicLong> holders) {
            this.holders = holders;
        }

        /** Sets every status to the given count; statuses absent from the map drop to zero. */
        public void refresh(final Map<E, Long> counts) {
            holders.forEach((status, holder) -> holder.set(counts.getOrDefault(status, 0L)));
        }
    }

    public static final class CounterBuilder {

        private final MeterRegistry registry;
        private final String name;
        private final String description;
        private final List<Tag> tags = new ArrayList<>();

        private CounterBuilder(final MeterRegistry registry, final String name, final String description) {
            this.registry = registry;
            this.name = name;
            this.description = description;
        }

        public CounterBuilder tag(final String key, final String value) {
            this.tags.add(Tag.of(key, value));
            return this;
        }

        public Counter register() {
            final Counter.Builder builder = Counter.builder(name).tags(tags);
            if (description != null) {
                builder.description(description);
            }
            return builder.register(registry);
        }
    }

    public static final class TimerBuilder {

        private final MeterRegistry registry;
        private final String name;
        private final String description;
        private final List<Tag> tags = new ArrayList<>();

        private TimerBuilder(final MeterRegistry registry, final String name, final String description) {
            this.registry = registry;
            this.name = name;
            this.description = description;
        }

        public TimerBuilder tag(final String key, final String value) {
            this.tags.add(Tag.of(key, value));
            return this;
        }

        public Timer register() {
            final Timer.Builder builder = Timer.builder(name).tags(tags);
            if (description != null) {
                builder.description(description);
            }
            return builder.register(registry);
        }
    }
}
