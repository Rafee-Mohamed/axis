package consensus.algorithm;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Tracks in-flight AppendEntries messages for flow control.
 * 
 * When a leader sends log entries to a follower, it doesn't wait for each
 * message to be acknowledged before sending the next (pipelining). Inflight
 * limits how many messages can be outstanding to prevent overwhelming slow
 * followers.
 * 
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────────────────────┐
 * │                          Inflight: Flow Control Buffer                          │
 * ├─────────────────────────────────────────────────────────────────────────────────┤
 * │                                                                                 │
 * │  Conceptually tracks messages sent but not yet acknowledged:                    │
 * │                                                                                 │
 * │    Leader Log:  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]                                 │
 * │                         ↑              ↑                                        │
 * │                       match           next                                      │
 * │                        =4             =11                                       │
 * │                                                                                 │
 * │    Messages sent (each covers entries up to its index):                         │
 * │      Msg1: up to index 6 (100 bytes)                                            │
 * │      Msg2: up to index 8 (150 bytes)                                            │
 * │      Msg3: up to index 10 (200 bytes)                                           │
 * │                                                                                 │
 * │    inflightBytes = 450                                                          │
 * │    entries.size() = 3                                                           │
 * │                                                                                 │
 * └─────────────────────────────────────────────────────────────────────────────────┘
 * </pre>
 * 
 * Two limits control when we stop sending:
 * - maxInflightMessages: Max number of unacknowledged messages (prevents queue explosion)
 * - maxInflightBytes: Max total bytes of unacknowledged entries (prevents memory pressure).
 *   Use Long.MAX_VALUE for unlimited.
 * 
 * isFull() returns true when EITHER limit is reached.
 * 
 * Lifecycle:
 * 1. add(index, bytes)           - Track new message
 * 2. removeMessagesUpto(index)   - Free acknowledged messages
 * 3. reset()                     - Clear all (state transition, error)
 * 
 * Implementation uses Java's ArrayDeque as a FIFO queue. Messages are acknowledged
 * in order (TCP guarantees ordering), so we always remove from the front.
 */
public class Inflight {
    
    /**
     * Represents a single in-flight message entry.
     * 
     * @param index the highest log index included in this message
     * @param bytes the total byte size of entries in this message
     */
    public record Entry(long index, long bytes) {}
    
    /**
     * Configuration for flow control limits.
     * 
     * @param maxInflightMessages maximum number of unacknowledged messages
     * @param maxInflightBytes    maximum total bytes in flight (use Long.MAX_VALUE for unlimited)
     */
    public record Config(int maxInflightMessages, long maxInflightBytes) {}

    /** Total bytes currently in flight. */
    private long inflightBytes;
    
    /** FIFO queue of in-flight entries. Front = oldest (acknowledged first). */
    private final Queue<Entry> entries;

    /** Flow control configuration. */
    private final Config config;
    
    /**
     * Creates a new Inflight tracker with the given configuration.
     *
     * @param config flow control limits
     */
    public Inflight(Config config) {
        this.config = config;
        this.inflightBytes = 0;
        this.entries = new ArrayDeque<>();
    }

    /**
     * Adds a new in-flight message. Call this after sending AppendEntries.
     *
     * Example:
     *   Before: count=2, bytes=250, entries=[(5,100), (7,150)]
     *   add(10, 200)
     *   After:  count=3, bytes=450, entries=[(5,100), (7,150), (10,200)]
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
     * Frees all messages with index <= the given index.
     * Call this when receiving a successful AppendEntriesResponse.
     *
     * Since acknowledgments are cumulative (acknowledging index N means all
     * entries up to N are replicated), we remove from the front until we
     * find an entry with index > the acknowledged index.
     *
     * Example:
     *   Before: entries=[(5,100), (7,150), (10,200)], bytes=450
     *   removeMessagesUpto(7)
     *   After:  entries=[(10,200)], bytes=200
     *
     * @param index the acknowledged index (all messages up to this are confirmed)
     */
    public void removeMessagesUpto(long index) {
        while (!entries.isEmpty() && entries.peek().index() <= index) {
            inflightBytes -= entries.poll().bytes();
        }
    }

    /**
     * Returns true if either the message count or byte limit is reached.
     * When full, the leader should pause sending until acknowledgments arrive.
     *
     * @return true if at capacity
     */
    public boolean isFull() {
        return entries.size() >= config.maxInflightMessages() || inflightBytes >= config.maxInflightBytes();
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
