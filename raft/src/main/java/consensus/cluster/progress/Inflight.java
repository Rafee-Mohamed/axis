package consensus.cluster.progress;

import consensus.config.PeerInflightConfig;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Tracks in-flight AppendEntries messages for flow control.
 *
 * <p>When a leader sends log entries to a follower, it doesn't wait for each
 * message to be acknowledged before sending the next (pipelining). Inflight
 * limits how many messages can be outstanding to prevent overwhelming slow
 * followers.</p>
 *
 * <pre>
 * +-----------------------------------------------------------------------+
 * |                    Inflight: Flow Control Buffer                      |
 * +-----------------------------------------------------------------------+
 * |                                                                       |
 * |  Leader Log:  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]                         |
 * |                        ^              ^                               |
 * |                      match           next                             |
 * |                       =4             =11                              |
 * |                                                                       |
 * |  Messages sent (each covers entries up to its index):                 |
 * |    Msg1: up to index 6  (100 bytes)                                   |
 * |    Msg2: up to index 8  (150 bytes)                                   |
 * |    Msg3: up to index 10 (200 bytes)                                   |
 * |                                                                       |
 * |  inflightBytes = 450                                                  |
 * |  entries.size() = 3                                                   |
 * |                                                                       |
 * +-----------------------------------------------------------------------+
 * </pre>
 *
 * <p>Two limits control when we stop sending:</p>
 * <ul>
 *   <li>{@code maxInflightMessages} - max number of unacknowledged messages
 *       (prevents queue explosion)</li>
 *   <li>{@code maxInflightBytes} - max total bytes of unacknowledged entries
 *       (prevents memory pressure). Use {@code Long.MAX_VALUE} for unlimited.</li>
 * </ul>
 *
 * <p>{@link #isFull()} returns {@code true} when either limit is reached.</p>
 *
 * <p>Lifecycle:</p>
 * <ol>
 *   <li>{@link #add(long, long)} - track a new message</li>
 *   <li>{@link #removeMessagesUpto(long)} - free acknowledged messages</li>
 *   <li>{@link #reset()} - clear all (state transition, error)</li>
 * </ol>
 *
 * <p>Implementation uses {@link java.util.ArrayDeque} as a FIFO queue. Messages
 * are acknowledged in order (TCP guarantees ordering), so we always remove
 * from the front.</p>
 */
public class Inflight {
    
    /**
     * Represents a single in-flight message entry.
     * 
     * @param index the highest log index included in this message
     * @param bytes the total byte size of entries in this message
     */
    public record Entry(long index, long bytes) {}
    
    /** Total bytes currently in flight. */
    private long inflightBytes;
    
    /** FIFO queue of in-flight entries. Front = oldest (acknowledged first). */
    private final Queue<Entry> entries;

    /** Flow control configuration. */
    private final PeerInflightConfig config;
    
    /**
     * Creates a new Inflight tracker with the given configuration.
     *
     * @param config flow control limits
     */
    public Inflight(PeerInflightConfig config) {
        this.config = config;
        this.inflightBytes = 0;
        this.entries = new ArrayDeque<>();
    }

    /**
     * Adds a new in-flight message. Call this after sending AppendEntries.
     *
     * <pre>
     * Example:
     *   Before: count=2, bytes=250, entries=[(5,100), (7,150)]
     *   add(10, 200)
     *   After:  count=3, bytes=450, entries=[(5,100), (7,150), (10,200)]
     * </pre>
     *
     * @param index the highest log index in this message
     * @param bytes total bytes of entries in this message
     * @throws IllegalStateException if already full
     */
    public void add(long index, long bytes) {
        if (isFull()) {
            throw new IllegalStateException("Cannot add inflight messages as messages or size limits reached");
        }

        entries.offer(new Entry(index, bytes));
        inflightBytes += bytes;
    }

    /**
     * Frees all messages with index &lt;= the given index.
     *
     * <p>Call this when receiving a successful AppendEntriesResponse.
     * Since acknowledgments are cumulative (acknowledging index N means all
     * entries up to N are replicated), we remove from the front until we
     * find an entry with index &gt; the acknowledged index.</p>
     *
     * <pre>
     * Example:
     *   Before: entries=[(5,100), (7,150), (10,200)], bytes=450
     *   removeMessagesUpto(7)
     *   After:  entries=[(10,200)], bytes=200
     * </pre>
     *
     * @param index the acknowledged index (all messages up to this are confirmed)
     */
    public void removeMessagesUpto(long index) {
        while (!entries.isEmpty() && entries.peek().index() <= index) {
            inflightBytes -= entries.poll().bytes();
        }
    }

    /**
     * Returns {@code true} if either the message count or byte limit is reached.
     *
     * <p>When full, the leader should pause sending until acknowledgments arrive.</p>
     *
     * @return {@code true} if at capacity
     */
    public boolean isFull() {
        return entries.size() >= config.maxInflightMsgs() || inflightBytes >= config.maxInflightBytes();
    }

    /**
     * Returns the number of in-flight messages.
     *
     * @return message count
     */
    public int count() {
        return entries.size();
    }

    /**
     * Returns the total bytes of in-flight entries.
     *
     * @return total bytes
     */
    public long bytes() {
        return inflightBytes;
    }

    /**
     * Clears all in-flight tracking. Called on state transitions
     * (e.g., stepping down as leader) or error recovery.
     */
    public void reset() {
        entries.clear();
        inflightBytes = 0;
    }
}
