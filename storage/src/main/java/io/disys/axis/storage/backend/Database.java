package io.disys.axis.storage.backend;

@FunctionalInterface
public interface Database {
    String name();

    static Database of(String name) {
        return () -> name;
    }
}