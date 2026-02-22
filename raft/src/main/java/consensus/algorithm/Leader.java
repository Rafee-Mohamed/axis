package consensus.algorithm;

import consensus.membership.MatchIndexer;
import consensus.message.Message;
import consensus.node.NodeId;
import consensus.storage.Entry;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public final class Leader implements Role {
    private final Map<NodeId, PeerProgress> peerProgress;
    private final NodeId id;
    private final Set<NodeId> peers;
    private final MatchIndexer matchIndexer;
    private final List<Message.ReadIndex> pendingReadIndexMessages;
    private long uncommittedSize;
    private final long maxUncommittedSize;
    private Optional<NodeId> transferTarget;
    private long membershipChangeIndex;
    private final boolean checkQuorum;
    private final TickTimer quorumCheckTimer;
    private final TickTimer heartbeatTimer;

    public Leader(NodeId leaderId, Set<NodeId> peers, long lastIndex, Inflight.Config inflightConfig) {
        id = leaderId;
        peerProgress = peers.stream().collect(Collectors.toMap((id) -> id, (_) -> new PeerProgress(inflightConfig)));
        // Leader's own progress starts in Replicate state and is always active.
        var leaderProgress = peerProgress.computeIfAbsent(leaderId, (_) -> new PeerProgress(inflightConfig));
        leaderProgress.becomeReplicate();
        leaderProgress.setActive(true);
        // Pre-compute the set of peers excluding ourselves for broadcast loops.
        this.peers = peers.stream().filter(Predicate.not(leaderId::equals)).collect(Collectors.toUnmodifiableSet());
        matchIndexer = new PeerMatchIndexer(peerProgress);
        transferTarget = Optional.empty();
        membershipChangeIndex = lastIndex;
        uncommittedSize = 0;
        maxUncommittedSize = 0;
        pendingReadIndexMessages = new ArrayList<>();
        checkQuorum = false;
        quorumCheckTimer = new TickTimer(10);
        heartbeatTimer = new TickTimer(10);

    }


    public boolean canCheckQuorumAfterTick() {
        if (!checkQuorum)
            return false;

        return quorumCheckTimer.resetIfTimedOutAfterTick();
    }

    public boolean canSendHeartBeatAfterTick() {
        return heartbeatTimer.resetIfTimedOutAfterTick();
    }

    public void abortLeaderTransfer() {
        transferTarget = Optional.empty();
    }

    public boolean tryIncreaseUncommittedSize(List<Entry> entries) {
        var size = Entry.calculateSize(entries);
        if ((uncommittedSize + size) > maxUncommittedSize)
            return false;

        uncommittedSize += size;
        return true;
    }

    public MatchIndexer matchIndexer() {
        return matchIndexer;
    }

    public boolean isLeaderTransferee(NodeId id) {
        return transferTarget.map(id::equals).orElse(false);
    }

    public boolean isLeaderTransferInProgress() {
        return transferTarget.isPresent();
    }


    public PeerProgress progress(NodeId id) {
        return peerProgress.get(id);
    }


    public Set<NodeId> peers() {
        return peers;
    }


    public Map<NodeId, Boolean> getQuorumVotesAndDeactivate() {
        var votes = new HashMap<NodeId, Boolean>(peerProgress.size());
        // Leader is always active — it wouldn't be running this check otherwise.
        votes.put(id, true);
        for (var peer: peers) {
            var progress = peerProgress.get(peer);
            votes.put(peer, progress.isActive());
            // Reset for next cycle: peer must respond again to be counted active.
            progress.setActive(false);
        }
        return votes;
    }

    // Implicitly aborts any previous in-progress transfer to a different target.
    public void transferLeadership(NodeId transferee) {
        transferTarget = Optional.of(transferee);
        // Reset to give the transferee a full election-timeout window to catch up.
        quorumCheckTimer.reset();
    }

    public boolean isQuorumCheckTimedOut() {
        return quorumCheckTimer.isTimedOut();
    }
}
