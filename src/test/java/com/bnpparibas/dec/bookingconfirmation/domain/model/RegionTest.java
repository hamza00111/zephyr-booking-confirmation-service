package com.bnpparibas.dec.bookingconfirmation.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RegionTest {

    @Test
    void values_areTheThreeRegions() {
        assertThat(Region.values()).containsExactly(Region.AMER, Region.APAC, Region.EMEA);
    }

    @Test
    void valueOf_resolvesByName() {
        assertThat(Region.valueOf("EMEA")).isEqualTo(Region.EMEA);
    }
}
