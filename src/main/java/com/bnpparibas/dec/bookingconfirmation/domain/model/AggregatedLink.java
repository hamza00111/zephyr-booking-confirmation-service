package com.bnpparibas.dec.bookingconfirmation.domain.model;

import org.jspecify.annotations.Nullable;

/**
 * Lineage of one inbox row collapsed away by aggregation: {@code aggregatedIntoId} is the ID of
 * the surviving (PROCESSED) inbox row whose event absorbed it, or {@code null} when the whole
 * group netted out (created and busted within one drain — nothing survived to point at).
 */
public record AggregatedLink(long id, @Nullable Long aggregatedIntoId) {}
