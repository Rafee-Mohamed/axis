package consensus.node;

import consensus.algorithm.Snapshot;
import consensus.message.Message;
import consensus.storage.Entry;

import java.util.List;

public interface RaftOutput {
    record Ready(
            // Soft state (leader, role) - for info only
            VolatileState volatileState,           // null if unchanged

            // Hard state - MUST persist
            PersistentState persistentState,           // null if unchanged

            // Entries to persist (append to log)
            List<Entry> entries,           // empty if none

            // Snapshot to persist
            Snapshot snapshot,             // null if none

            // Messages to send (AFTER persisting)
            List<Message> messages,

            // Committed entries to apply to state machine
            List<Entry> committedEntries,

            // Read states for read-only queries
            List<ReadState> readStates
    ) implements RaftOutput {}
}
