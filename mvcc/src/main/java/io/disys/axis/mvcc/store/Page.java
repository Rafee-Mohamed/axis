package io.disys.axis.mvcc.store;

import java.util.List;

public record Page<T>(List<T> items, boolean more) {}
