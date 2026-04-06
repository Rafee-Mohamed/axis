package io.disys.axis.backend;

public interface Database {
    String name();
    static Database of(String name) {
        return () -> name;
    }
}