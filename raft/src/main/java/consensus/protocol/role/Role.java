package consensus.protocol.role;

public sealed interface Role permits Leader, Replicant, Candidate, PreCandidate {
    RoleType type();
}
