package io.disys.axis.mvcc.model;

import java.util.List;

/**
 * A paginated result set.
 *
 * @param items  the records or keys returned in this page
 * @param more   {@code true} if the result was truncated by a limit and
 *               additional results exist beyond this page
 * @param <T>    the element type
 */
public record Page<T>(List<T> items, boolean more) {}
