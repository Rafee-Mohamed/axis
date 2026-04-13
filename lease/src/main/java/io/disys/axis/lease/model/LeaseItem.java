package io.disys.axis.lease.model;

import java.util.Arrays;

public record LeaseItem(byte[] key) {

    @Override
    public boolean equals(Object o) {
        return o instanceof LeaseItem(byte[] other) && Arrays.equals(this.key, other);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }
}
