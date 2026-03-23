package io.disys.axis.storage.backend;

public record Database(String name) {
    static Database of(String name) {
        return new Database(name);
    }
}