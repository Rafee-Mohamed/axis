package consensus.storage;

import consensus.membership.MembershipChanges;

import java.util.List;

sealed public interface Entry {
    long term();
    long index();

    record Id(long term, long index) {};
    default long size() { return 0; };

    static long calculateSize(List<Entry> entries) {
        return entries.stream().mapToLong(Entry::size).sum();
    }
    record Placeholder(long term, long index) implements Entry {}
    record Data(long term, long index, Payload data) implements Entry {
        @Override
        public long size() { return data.bytes(); }
    }
    record MembershipChange(long term, long index, MembershipChanges membershipChanges) implements Entry {}
    record LeaveJoint(long term, long index) implements Entry {};
}

