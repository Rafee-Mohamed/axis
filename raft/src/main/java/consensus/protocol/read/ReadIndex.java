package consensus.protocol.read;

import consensus.core.NodeId;
import consensus.cluster.membership.MembershipConfig;

import java.util.*;

/**
 * Tracks pending linearizable read requests waiting for leadership confirmation
 * via heartbeat sequence numbers.
 *
 * <h2>State</h2>
 * <pre>
 *   seq            - monotonic counter, incremented once per heartbeat broadcast
 *   pendingReads   - FIFO queue of reads waiting for majority ack
 *   peerAckedSeq   - per-peer high-water mark of acked heartbeat seq
 * </pre>
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li>Read arrives        - addPending(from, commitIndex), stores Pending{from, commitIndex, seq+1}</li>
 *   <li>Leader broadcasts HB - nextSeq() increments seq, attaches it to HB message</li>
 *   <li>Followers echo seq   - onHeartbeatAck(peer, ackedSeq) records the ack</li>
 *   <li>Majority acked?      - drainAcked() confirms and returns reads</li>
 * </ol>
 *
 * <h2>Pending Queue</h2>
 * <pre>
 *   seq = 2 at the time reads arrive, then two heartbeat broadcasts happen:
 *
 *   R1, R2, R3 arrive (seq=2) -> stored with requiredSeq = 3  (seq+1)
 *   broadcastHeartbeat()       -> seq becomes 3 (nextSeq()), HB(seq=3) sent
 *   R4, R5 arrive (seq=3)     -> stored with requiredSeq = 4  (seq+1)
 *   broadcastHeartbeat()       -> seq becomes 4 (nextSeq()), HB(seq=4) sent
 *
 *   requiredSeq:  3       3       3       4       4
 *              ┌───────┬───────┬───────┬───────┬───────┐
 *              │  R1   │  R2   │  R3   │  R4   │  R5   │
 *              └───────┴───────┴───────┴───────┴───────┘
 *                front                             back
 *                 ↑                        ↑
 *            drain when               drain when
 *          majorityAcked >= 3       majorityAcked >= 4
 * </pre>
 *
 * <h2>Why seq + 1</h2>
 * <pre>
 *   Problem: a read arrives when seq = 5.
 *            Heartbeat with seq=5 was already sent BEFORE this read.
 *            Acks for seq=5 prove leadership at the time of that HB,
 *            not after the read was registered.
 *
 *   Fix:     store requiredSeq = seq + 1 = 6.
 *            The read is only confirmed by HB(seq=6) or later,
 *            which is guaranteed to be sent AFTER the read arrived.
 *
 *   Timeline:
 *     HB(seq=5) sent ... read arrives (seq=5) ... HB(seq=6) sent ... acks for 6 arrive
 *                         |                                            |
 *                    requiredSeq = 6                            confirmed here
 * </pre>
 *
 * <h2>How drainAcked Works</h2>
 * <pre>
 *   3-node cluster: Leader(A), B, C
 *
 *   peerAckedSeq:  { B: 3, C: 0 }     (B acked seq=3, C hasn't responded)
 *   leader A:      Long.MAX_VALUE      (leader always counts itself)
 *
     *   Step 1 - compute majorityAckedSeq (same algorithm as majorityAgreed):
 *
 *     Sorted acked values:  [0,  3,  MAX]
 *                                ↑
 *                            majority position = length(3) - majority(2) = index 1
 *                            majorityAckedSeq = 3
 *
 *   Step 2 - drain from front while pending.seq <= majorityAckedSeq:
 *
 *     requiredSeq:  3       3       3       4       4
 *                ┌───────┬───────┬───────┬───────┬───────┐
 *                │  R1   │  R2   │  R3   │  R4   │  R5   │
 *                └───────┴───────┴───────┴───────┴───────┘
 *                 3 <= 3   3 <= 3   3 <= 3  4 > 3  stop
 *                |-------- drain --------||---- stay ----|
 *
 *     Result: R1, R2, R3 confirmed. R4, R5 stay in queue.
 *
 *   Later, C acks seq=4:
 *     peerAckedSeq: { B: 3, C: 4 }
 *     Sorted: [3, 4, MAX] -> majorityAckedSeq = 4
 *
 *                ┌───────┬───────┐
 *                │  R4   │  R5   │
 *                └───────┴───────┘
 *                 4 <= 4   4 <= 4  -> both drained
 * </pre>
 *
 * <h2>Why <= (not &lt;) for the drain check</h2>
 * <p>The check is {@code pending.seq <= majorityAckedSeq}. A pending read
 * with requiredSeq = N needs majority to have acked at least seq N. If
 * majorityAckedSeq = N, that means majority has seen HB(seq=N) or later.
 * That heartbeat was sent after the read was registered (because of the
 * seq+1 assignment), so leadership is confirmed. Equal is sufficient.</p>
 *
 * <h2>Invariants</h2>
 * <ul>
 *   <li>seq is strictly monotonically increasing (incremented once per broadcast)</li>
 *   <li>Pending.seq values in the queue are monotonically non-decreasing
 *       (FIFO ordering + seq only grows -> earlier entries have <= seq)</li>
 *   <li>peerAckedSeq[peer] never decreases (Math.max on every update)</li>
 *   <li>Once a Pending can't be drained (its seq > majorityAcked), no later
 *       Pending can either, so we stop draining at the first failure</li>
 *   <li>All pending reads belong to a single term. ReadIndex is created fresh
 *       when a node becomes leader. On term change (leader steps down), the
 *       old ReadIndex along with all its pending reads is discarded. A new
 *       leader starts with seq=0, empty queue, empty peerAckedSeq. There is
 *       no cross-term contamination.</li>
 * </ul>
 */
public class ReadIndex {

    /**
     * A pending read awaiting leadership confirmation.
     *
     * @param from  node that requested the read (leader's own id or a follower's id)
     * @param index committed index when registered - becomes the readIndex for the application
     * @param seq   minimum heartbeat seq that majority must ack to confirm this read
     */
    public record Pending(NodeId from, long index, long seq) {};

    // Monotonic heartbeat sequence counter. Incremented once per broadcast.
    private long seq;

    // FIFO queue of pending reads, ordered by non-decreasing seq.
    private final Queue<Pending> pendingReads;

    // Per-peer high-water mark: highest heartbeat seq each peer has acked.
    private final Map<NodeId, Long> peerAckedSeq;

    public ReadIndex() {
        seq = 0;
        pendingReads = new ArrayDeque<>();
        peerAckedSeq = new HashMap<>();
    }

    /**
     * Increments and returns the heartbeat sequence number.
     * Called when broadcasting a heartbeat — the returned value is attached
     * to the heartbeat message and echoed back by followers.
     */
    public long nextSeq() {
        return ++seq;
    }

    /**
     * Registers a pending read at the current commit index.
     *
     * <p>The read records {@code seq + 1} as its required confirmation seq.
     * This ensures the read is only confirmed by a heartbeat sent
     * <em>after</em> this registration — not by a stale response from
     * a heartbeat sent before the read arrived.</p>
     *
     * @param id        the requesting node (leader's own id for local reads,
     *                  follower id for forwarded reads)
     * @param readIndex the leader's commit index at registration time
     */
    public void addPending(NodeId id, long readIndex) {
        pendingReads.add(new Pending(id, readIndex, seq + 1));
    }

    /**
     * Records a peer's heartbeat acknowledgment.
     * Uses {@code Math.max} so the tracked seq never goes backwards —
     * out-of-order responses are handled correctly.
     */
    public void onHeartbeatAck(NodeId id, long ackedSeq) {
        peerAckedSeq.merge(id, ackedSeq, Math::max);
    }

    /**
     * Drains all pending reads that have been confirmed by a majority of
     * heartbeat acknowledgments.
     *
     * <p>Computes the majority-agreed seq using the same sorted-majority
     * algorithm as {@link MembershipConfig#majorityAgreed} — sort each
     * voter's acked seq, pick the value at the majority position. This
     * gives the highest seq that at least a majority has acknowledged.
     * The leader is always counted as having acked {@code Long.MAX_VALUE}
     * (it trivially confirms its own leadership).</p>
     *
     * <p>Then drains the FIFO queue from the front: all pending reads
     * whose required seq {@code <=} the majority-agreed seq are confirmed.
     * Since the queue is monotonically non-decreasing by seq, this is
     * always correct — once a read can't be confirmed, no later read
     * can either.</p>
     *
     * @param leader the leader's node id
     * @param mc     the current membership config (for quorum calculation)
     * @return confirmed reads to respond to (empty list if none)
     */
    public List<Pending> drainAcked(NodeId leader, MembershipConfig mc) {
        if (pendingReads.isEmpty()) {
            return List.of();
        }

        var quorumAckedSeq = mc.quorumAgreed(voter ->
                voter.equals(leader) ? Long.MAX_VALUE : peerAckedSeq.getOrDefault(voter, 0L),
                Long.MAX_VALUE
        );

        var ackedReads = new ArrayList<Pending>();
        while (!pendingReads.isEmpty() && pendingReads.peek().seq() <= quorumAckedSeq) {
            ackedReads.add(pendingReads.poll());
        }
        return ackedReads;
    }


}
