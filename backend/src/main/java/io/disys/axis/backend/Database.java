package io.disys.axis.backend;

public interface Database {
    String name();

    static Database of(String name) {
        return new Impl(name);
    }

    record Impl(String name) implements Database {}
}