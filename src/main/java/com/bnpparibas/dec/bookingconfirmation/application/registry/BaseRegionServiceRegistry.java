package com.bnpparibas.dec.bookingconfirmation.application.registry;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.RegionProperties;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingConfirmationService;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;

/**
 * Builds and holds one region-scoped service instance per active region for a single stage.
 *
 * <p>{@link #initialize()} is called once at startup (in dependency order) by the scheduler
 * initializer; the registry is read-only afterwards.
 */
public abstract class BaseRegionServiceRegistry<S extends BookingConfirmationService> {

    private final BookingConfirmationProperties properties;
    private final Map<Region, S> services = new EnumMap<>(Region.class);

    protected BaseRegionServiceRegistry(final BookingConfirmationProperties properties) {
        this.properties = properties;
    }

    public void initialize() {
        for (final Region region : properties.activeRegions()) {
            services.put(region, buildService(region, properties.region(region)));
        }
    }

    protected abstract S buildService(Region region, RegionProperties regionProperties);

    public Collection<S> services() {
        return services.values();
    }

    public S service(final Region region) {
        return services.get(region);
    }

    protected BookingConfirmationProperties properties() {
        return properties;
    }
}
