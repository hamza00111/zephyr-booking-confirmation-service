package com.bnpparibas.dec.bookingconfirmation.domain.repository;

import com.bnpparibas.dec.bookingconfirmation.domain.model.CreateState;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.util.Collection;
import java.util.Map;

/**
 * Persistence contract for the create-barrier ledger (one row per {@code (region, message key)}).
 *
 * <p>Reads/writes are region-scoped and run inside the PROCESS or RELAY transaction. Because PROCESS
 * runs single-writer per region (region lock + single-threaded tick), gate upserts for a key never
 * race within a region, and regions never share keys.
 */
public interface TradeGateRepository {

    /**
     * Bulk-reads the current state of the given keys. Keys with no row are absent from the map and
     * must be treated as {@link CreateState#NONE} by the caller.
     */
    Map<String, CreateState> statesFor(Region region, Collection<String> messageKeys);

    /** Upserts the gate state for a single key. */
    void upsert(Region region, String messageKey, CreateState state);

    /**
     * Sets the given keys to {@link CreateState#SENT} (called by RELAY when a CREATE is delivered).
     *
     * @return the keys whose state actually transitioned to SENT (i.e. were not already SENT).
     */
    void markSent(Region region, Collection<String> messageKeys);
}
