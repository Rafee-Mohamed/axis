package io.disys.axis.consensus.transport.codec;

import com.google.protobuf.InvalidProtocolBufferException;
import io.disys.axis.consensus.RaftPayload;
import io.disys.axis.consensus.log.WalDecodeException;
import io.disys.axis.consensus.proto.AppendEntries;
import io.disys.axis.consensus.proto.AppendEntriesResponse;
import io.disys.axis.consensus.proto.DataProposal;
import io.disys.axis.consensus.proto.Heartbeat;
import io.disys.axis.consensus.proto.HeartbeatResponse;
import io.disys.axis.consensus.proto.InstallSnapshot;
import io.disys.axis.consensus.proto.LeaveJointProposal;
import io.disys.axis.consensus.proto.MembershipChangeProposal;
import io.disys.axis.consensus.proto.PeerMessage;
import io.disys.axis.consensus.proto.ReadIndex;
import io.disys.axis.consensus.proto.ReadIndexResponse;
import io.disys.axis.consensus.proto.RequestPreVote;
import io.disys.axis.consensus.proto.RequestPreVoteResponse;
import io.disys.axis.consensus.proto.RequestVote;
import io.disys.axis.consensus.proto.RequestVoteResponse;
import io.disys.axis.consensus.proto.TimeoutNow;
import io.disys.axis.consensus.proto.TransferLeadership;
import io.disys.jaft.core.NodeId;
import io.disys.jaft.message.Message;
import io.disys.jaft.protocol.policy.ElectionCause;

// NOTE: proto ElectionCause conflicts with jaft ElectionCause — proto version is fully qualified.
public final class PeerMessageCodec {

    private PeerMessageCodec() {}

    // ===================== ElectionCause =====================================

    private static io.disys.axis.consensus.proto.ElectionCause encodeElectionCause(ElectionCause cause) {
        return switch (cause) {
            case ELECTION_TIMEOUT       -> io.disys.axis.consensus.proto.ElectionCause.ELECTION_CAUSE_TIMEOUT;
            case LEADER_TRANSFER        -> io.disys.axis.consensus.proto.ElectionCause.ELECTION_CAUSE_LEADER_TRANSFER;
            case WON_PREELECTION        -> io.disys.axis.consensus.proto.ElectionCause.ELECTION_CAUSE_WON_PREELECTION;
            case ELECTION_ROUND_TIMEOUT -> io.disys.axis.consensus.proto.ElectionCause.ELECTION_CAUSE_ROUND_TIMEOUT;
        };
    }

    private static ElectionCause decodeElectionCause(io.disys.axis.consensus.proto.ElectionCause proto) {
        return switch (proto) {
            case ELECTION_CAUSE_TIMEOUT         -> ElectionCause.ELECTION_TIMEOUT;
            case ELECTION_CAUSE_LEADER_TRANSFER -> ElectionCause.LEADER_TRANSFER;
            case ELECTION_CAUSE_WON_PREELECTION -> ElectionCause.WON_PREELECTION;
            case ELECTION_CAUSE_ROUND_TIMEOUT   -> ElectionCause.ELECTION_ROUND_TIMEOUT;
            default -> throw new IllegalArgumentException("Unrecognized ElectionCause: " + proto);
        };
    }

    // ===================== Encode ============================================

    public static PeerMessage encode(Message.Peer message) {
        return switch (message) {
            case Message.AppendEntries m -> PeerMessage.newBuilder()
                    .setAppendEntries(AppendEntries.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .setTerm(m.term())
                            .setPrevLogTerm(m.prevLogTerm())
                            .setPrevLogIndex(m.prevLogIndex())
                            .addAllEntries(m.entries().stream().map(EntryCodec::encode).toList())
                            .setLeaderCommit(m.leaderCommit())
                            .build())
                    .build();

            case Message.AppendEntriesResponse m -> PeerMessage.newBuilder()
                    .setAppendEntriesResponse(AppendEntriesResponse.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .setTerm(m.term())
                            .setSuccess(m.success())
                            .setIndex(m.index())
                            .setTermHint(m.termHint())
                            .setIndexHint(m.indexHint())
                            .build())
                    .build();

            case Message.InstallSnapshot m -> PeerMessage.newBuilder()
                    .setInstallSnapshot(InstallSnapshot.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .setTerm(m.term())
                            .setSnapshot(SnapshotCodec.encode(m.snapshot()))
                            .build())
                    .build();

            case Message.Heartbeat m -> PeerMessage.newBuilder()
                    .setHeartbeat(Heartbeat.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .setTerm(m.term())
                            .setLeaderCommit(m.leaderCommit())
                            .setSequence(m.sequence())
                            .build())
                    .build();

            case Message.HeartbeatResponse m -> PeerMessage.newBuilder()
                    .setHeartbeatResponse(HeartbeatResponse.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .setTerm(m.term())
                            .setSequence(m.sequence())
                            .build())
                    .build();

            case Message.RequestVote m -> PeerMessage.newBuilder()
                    .setRequestVote(RequestVote.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .setTerm(m.term())
                            .setLastLogTerm(m.lastLogTerm())
                            .setLastLogIndex(m.lastLogIndex())
                            .setCause(encodeElectionCause(m.cause()))
                            .build())
                    .build();

            case Message.RequestVoteResponse m -> PeerMessage.newBuilder()
                    .setRequestVoteResponse(RequestVoteResponse.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .setTerm(m.term())
                            .setVoteGranted(m.voteGranted())
                            .build())
                    .build();

            case Message.RequestPreVote m -> PeerMessage.newBuilder()
                    .setRequestPreVote(RequestPreVote.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .setTerm(m.term())
                            .setLastLogTerm(m.lastLogTerm())
                            .setLastLogIndex(m.lastLogIndex())
                            .build())
                    .build();

            case Message.RequestPreVoteResponse m -> PeerMessage.newBuilder()
                    .setRequestPreVoteResponse(RequestPreVoteResponse.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .setTerm(m.term())
                            .setVoteGranted(m.voteGranted())
                            .build())
                    .build();

            case Message.TransferLeadership m -> PeerMessage.newBuilder()
                    .setTransferLeadership(TransferLeadership.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .setTransfereeId(m.transferee().id())
                            .build())
                    .build();

            case Message.TimeoutNow m -> PeerMessage.newBuilder()
                    .setTimeoutNow(TimeoutNow.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .setTerm(m.term())
                            .build())
                    .build();

            case Message.ReadIndex m -> PeerMessage.newBuilder()
                    .setReadIndex(ReadIndex.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .build())
                    .build();

            case Message.ReadIndexResponse m -> PeerMessage.newBuilder()
                    .setReadIndexResponse(ReadIndexResponse.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .setTerm(m.term())
                            .setReadIndex(m.readIndex())
                            .build())
                    .build();

            case Message.DataProposal m -> PeerMessage.newBuilder()
                    .setDataProposal(DataProposal.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .addAllData(m.data().stream()
                                    .map(p -> ((RaftPayload) p).data())
                                    .toList())
                            .build())
                    .build();

            case Message.MembershipChangeProposal m -> PeerMessage.newBuilder()
                    .setMembershipChangeProposal(MembershipChangeProposal.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .setChanges(MembershipCodec.encodeMembershipChanges(m.membershipChanges()))
                            .build())
                    .build();

            case Message.LeaveJointProposal m -> PeerMessage.newBuilder()
                    .setLeaveJointProposal(LeaveJointProposal.newBuilder()
                            .setTo(m.to().id())
                            .setFrom(m.from().id())
                            .build())
                    .build();
        };
    }

    // ===================== Decode ============================================

    public static Message.Peer decode(PeerMessage proto) {
        return switch (proto.getMessageCase()) {
            case APPEND_ENTRIES -> {
                var m = proto.getAppendEntries();
                yield new Message.AppendEntries(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom()),
                        m.getTerm(),
                        m.getPrevLogTerm(),
                        m.getPrevLogIndex(),
                        m.getEntriesList().stream().map(EntryCodec::decode).toList(),
                        m.getLeaderCommit()
                );
            }

            case APPEND_ENTRIES_RESPONSE -> {
                var m = proto.getAppendEntriesResponse();
                yield new Message.AppendEntriesResponse(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom()),
                        m.getTerm(),
                        m.getSuccess(),
                        m.getIndex(),
                        m.getTermHint(),
                        m.getIndexHint()
                );
            }

            case INSTALL_SNAPSHOT -> {
                var m = proto.getInstallSnapshot();
                yield new Message.InstallSnapshot(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom()),
                        m.getTerm(),
                        SnapshotCodec.decode(m.getSnapshot())
                );
            }

            case HEARTBEAT -> {
                var m = proto.getHeartbeat();
                yield new Message.Heartbeat(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom()),
                        m.getTerm(),
                        m.getLeaderCommit(),
                        m.getSequence()
                );
            }

            case HEARTBEAT_RESPONSE -> {
                var m = proto.getHeartbeatResponse();
                yield new Message.HeartbeatResponse(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom()),
                        m.getTerm(),
                        m.getSequence()
                );
            }

            case REQUEST_VOTE -> {
                var m = proto.getRequestVote();
                yield new Message.RequestVote(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom()),
                        m.getTerm(),
                        m.getLastLogTerm(),
                        m.getLastLogIndex(),
                        decodeElectionCause(m.getCause())
                );
            }

            case REQUEST_VOTE_RESPONSE -> {
                var m = proto.getRequestVoteResponse();
                yield new Message.RequestVoteResponse(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom()),
                        m.getTerm(),
                        m.getVoteGranted()
                );
            }

            case REQUEST_PRE_VOTE -> {
                var m = proto.getRequestPreVote();
                yield new Message.RequestPreVote(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom()),
                        m.getTerm(),
                        m.getLastLogTerm(),
                        m.getLastLogIndex()
                );
            }

            case REQUEST_PRE_VOTE_RESPONSE -> {
                var m = proto.getRequestPreVoteResponse();
                yield new Message.RequestPreVoteResponse(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom()),
                        m.getTerm(),
                        m.getVoteGranted()
                );
            }

            case TRANSFER_LEADERSHIP -> {
                var m = proto.getTransferLeadership();
                yield new Message.TransferLeadership(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom()),
                        new NodeId(m.getTransfereeId())
                );
            }

            case TIMEOUT_NOW -> {
                var m = proto.getTimeoutNow();
                yield new Message.TimeoutNow(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom()),
                        m.getTerm()
                );
            }

            case READ_INDEX -> {
                var m = proto.getReadIndex();
                yield new Message.ReadIndex(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom())
                );
            }

            case READ_INDEX_RESPONSE -> {
                var m = proto.getReadIndexResponse();
                yield new Message.ReadIndexResponse(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom()),
                        m.getTerm(),
                        m.getReadIndex()
                );
            }

            case DATA_PROPOSAL -> {
                var m = proto.getDataProposal();
                yield new Message.DataProposal(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom()),
                        m.getDataList().stream()
                                .map(bs -> {
                                    try {
                                        return RaftPayload.decode(bs);
                                    } catch (InvalidProtocolBufferException e) {
                                        throw new WalDecodeException(e);
                                    }
                                })
                                .toList()
                );
            }

            case MEMBERSHIP_CHANGE_PROPOSAL -> {
                var m = proto.getMembershipChangeProposal();
                yield new Message.MembershipChangeProposal(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom()),
                        MembershipCodec.decodeMembershipChanges(m.getChanges())
                );
            }

            case LEAVE_JOINT_PROPOSAL -> {
                var m = proto.getLeaveJointProposal();
                yield new Message.LeaveJointProposal(
                        new NodeId(m.getTo()),
                        new NodeId(m.getFrom())
                );
            }

            case MESSAGE_NOT_SET -> throw new IllegalArgumentException("PeerMessage has no message set");
        };
    }
}
