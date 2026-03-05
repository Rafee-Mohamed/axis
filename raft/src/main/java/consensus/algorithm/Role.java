package consensus.algorithm;

public sealed interface Role permits Leader, Follower, Candidate, PreCandidate, Learner {
    default RoleType type() {
        return switch (this) {
            case Candidate _ -> RoleType.CANDIDATE;
            case Follower _ -> RoleType.FOLLOWER;
            case Leader _ -> RoleType.LEADER;
            case Learner _ -> RoleType.LEARNER;
            case PreCandidate _ -> RoleType.PRE_CANDIDATE;
        };
    }
}
