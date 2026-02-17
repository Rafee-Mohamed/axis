package consensus.algorithm;

public sealed interface Role permits Leader, Follower, Candidate, PreCandidate, Learner {
}
