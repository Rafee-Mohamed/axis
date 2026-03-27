package io.disys.axis.storage.backend;

public interface Database {
    String name();
    static Database of(String name) {
        return () -> name;
    }
}