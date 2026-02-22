package consensus.algorithm;

import consensus.membership.MembershipConfig;

/**
 * A point-in-time capture of the state machine, used to bring slow peers
 * up to speed without replaying the entire log.
 *
 * <p>When a follower (or learner) is too far behind — the entries it needs
 * have been compacted from the leader's log — the leader sends a snapshot
 * instead of AppendEntries. The snapshot replaces the receiver's log up to
 * the snapshot point and provides the state machine data needed to resume
 * from that point.</p>
 *
 * <p>A snapshot also carries the cluster's membership configuration at the
 * time it was taken. This is essential because membership changes are log
 * entries, and those entries may have been compacted away. Without the
 * membership in the snapshot, a node restoring from it would have no way
 * to know who the current voters and learners are.</p>
 *
 * @param term       term of the last entry included in the snapshot
 * @param index      index of the last entry included in the snapshot
 *                   (all log entries at or before this index are represented
 *                   by the snapshot data)
 * @param membership the cluster membership configuration at the time the
 *                   snapshot was taken (voters, learners, joint state)
 * @param data       the serialized state machine data
 */
public record Snapshot (
        long term,
        long index,
        MembershipConfig membership,
        byte[] data
){
    /**
     * A zero-valued snapshot (index=0) is considered empty — no snapshot
     * has been taken or received yet.
     */
    public boolean isEmpty() {
        return index == 0;
    }
}
