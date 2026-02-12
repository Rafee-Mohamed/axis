package consensus.storage;

import java.util.List;
import java.util.stream.Collectors;

public record Entry(
        Type type,
        long term,
        long index,
        byte[] data
) {
    public enum Type {
        PLACEHOLDER,         // Marker entry (used at snapshot boundary, new leader entry)
        NORMAL,              // Regular data entry
        MEMBERSHIP_CHANGE    // Configuration change entry
    }

    public record Id(long term, long index){};

    // Factory methods
    public static Entry placeholder(long term, long index) {
        return new Entry(Type.PLACEHOLDER, term, index, null);
    }

    public static Entry normal(long term, long index, byte[] data) {
        return new Entry(Type.NORMAL, term, index, data);
    }

    public static Entry membershipChange(long term, long index, byte[] data) {
        return new Entry(Type.MEMBERSHIP_CHANGE, term, index, data);
    }

    public static long calculateSize(List<Entry> entries) {
        return entries.stream().mapToLong(Entry::size).sum();
    }

    /**
     * Estimate the size of an entry - enum type - int (4 bytes), term long (8 bytes), index long (8 bytes), data.length (bytes)
     * @return number of bytes required for entry
     */
    public long size() {
        return 8 + 8 + 4 + (data == null ? 0 : data.length);
    }
}


