package consensus.algorithm;

import consensus.message.Message;
import consensus.node.NodeId;
import consensus.storage.Entry;

import java.util.*;
import java.util.stream.Collectors;

public final class Leader implements Role {
    private final Map<NodeId, PeerProgress> peerProgress;
    private final List<Message.ReadIndex> pendingReadIndexMessages;
    private long uncommittedSize;
    private final long maxUncommittedSize;
    private Optional<NodeId> transferTarget;
    private long pendingConfIndex;
    private int heartbeatElapsed;
    private int quorumCheckElapsed;
    private int quorumCheckTimeout;
    private int heartBeatTimeout;
    private boolean checkQuorum;

    public Leader(NodeId leaderId, Set<NodeId> peers, Inflight.Config inflightConfig) {
        peerProgress = peers.stream().collect(Collectors.toMap((id) -> id, (_) -> new PeerProgress(inflightConfig)));
        var leaderProgress = peerProgress.get(leaderId);
        leaderProgress.becomeReplicate();
        leaderProgress.setActive(true);
        transferTarget = Optional.empty();
        pendingConfIndex = 0;
        uncommittedSize = 0;
        heartbeatElapsed = 0;
        maxUncommittedSize = 0;
        pendingReadIndexMessages = new ArrayList<>();
        checkQuorum = false;
    }

    public boolean canCheckQuorumAfterTick() {
        if (!checkQuorum)
            return false;

        quorumCheckElapsed++;
        var canCheckQuorum = quorumCheckElapsed >= quorumCheckTimeout;
        if (canCheckQuorum)
            quorumCheckElapsed = 0;

        return canCheckQuorum;
    }

    public boolean canSendHeartBeatAfterTick() {
        heartbeatElapsed++;
        var canSendHeartBeat = heartbeatElapsed >= heartBeatTimeout;
        if (canSendHeartBeat)
            heartbeatElapsed = 0;
        return canSendHeartBeat;
    }

    public void abortLeaderTransfer() {
        transferTarget = Optional.empty();
    }

    public boolean exceedUncommittedSize(List<Entry> entries) {
        var size = Entry.calculateSize(entries);
        return (uncommittedSize + size) > maxUncommittedSize;
    }


}
