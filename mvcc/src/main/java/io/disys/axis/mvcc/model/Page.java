package io.disys.axis.mvcc.model;

import java.util.List;

public record Page<T>(List<T> items, boolean more) {}
