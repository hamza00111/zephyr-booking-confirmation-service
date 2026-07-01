package com.bnpparibas.dec.bookingconfirmation.application.partition;

import static org.assertj.core.api.Assertions.assertThat;

import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import org.junit.jupiter.api.Test;

class OwnedPartitionsTest {

    private final OwnedPartitions ownedPartitions = new OwnedPartitions();

    @Test
    void forRegion_isEmpty_whenNothingOwned() {
        assertThat(ownedPartitions.forRegion(Region.EMEA)).isEmpty();
    }

    @Test
    void add_thenForRegion_returnsOwnedPartitions() {
        ownedPartitions.add(Region.EMEA, 2);
        ownedPartitions.add(Region.EMEA, 5);

        assertThat(ownedPartitions.forRegion(Region.EMEA)).containsExactlyInAnyOrder(2, 5);
    }

    @Test
    void add_isRegionScoped() {
        ownedPartitions.add(Region.EMEA, 2);
        ownedPartitions.add(Region.APAC, 2);

        assertThat(ownedPartitions.forRegion(Region.EMEA)).containsExactly(2);
        assertThat(ownedPartitions.forRegion(Region.APAC)).containsExactly(2);
    }

    @Test
    void remove_dropsOnlyThatPartition() {
        ownedPartitions.add(Region.EMEA, 2);
        ownedPartitions.add(Region.EMEA, 5);

        ownedPartitions.remove(Region.EMEA, 2);

        assertThat(ownedPartitions.forRegion(Region.EMEA)).containsExactly(5);
    }

    @Test
    void remove_isNoOp_whenNotOwned() {
        ownedPartitions.remove(Region.EMEA, 9);

        assertThat(ownedPartitions.forRegion(Region.EMEA)).isEmpty();
    }

    @Test
    void forRegion_returnsImmutableSnapshot() {
        ownedPartitions.add(Region.EMEA, 2);
        var snapshot = ownedPartitions.forRegion(Region.EMEA);

        ownedPartitions.add(Region.EMEA, 5);

        // snapshot taken before the second add is unaffected
        assertThat(snapshot).containsExactly(2);
    }
}
