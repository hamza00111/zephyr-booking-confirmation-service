package com.bnpparibas.dec.bookingconfirmation.application.service;

import com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetrics;
import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;

/**
 * The identity every region-scoped stage service shares: which region it drains, the live view of
 * the Kafka partitions this instance owns for it (ADR 0001), and where it records metrics.
 */
public record RegionScope(Region region, OwnedPartitions ownedPartitions, BookingConfirmationMetrics metrics) {}
