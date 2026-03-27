package io.disys.axis.backend.lmdb;

import io.disys.axis.storage.backend.Database;
import java.nio.file.Path;
import java.util.Set;

public record LmdbConfig(
        Path directory,
        long mapSize,
        Set<Database> databases,
        Path snapshotDestination
) {
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Path directory;
        private long mapSize = 10L * 1024 * 1024 * 1024; // 10GB default
        private Set<Database> databases = Set.of();
        private Path snapshotDestination;

        public Builder directory(Path directory) {
            this.directory = directory;
            return this;
        }

        public Builder mapSize(long mapSize) {
            this.mapSize = mapSize;
            return this;
        }

        public Builder databases(Set<Database> databases) {
            this.databases = databases;
            return this;
        }

        public Builder snapshotDestination(Path snapshotDestination) {
            this.snapshotDestination = snapshotDestination;
            return this;
        }

        public LmdbConfig build() {
            if (directory == null) throw new IllegalStateException("directory required");
            if (snapshotDestination == null) throw new IllegalStateException("snapshot destination required");
            if (databases.isEmpty()) throw new IllegalStateException("at least one database required");
            return new LmdbConfig(directory, mapSize, databases, snapshotDestination);
        }
    }
}