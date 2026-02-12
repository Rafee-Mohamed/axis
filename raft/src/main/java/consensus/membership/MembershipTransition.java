package consensus.membership;

public enum MembershipTransition {
    AUTO,           // Use simple if possible, otherwise joint with auto-leave
    JOINT_AUTO,     // Always use joint, auto-leave when safe
    JOINT_EXPLICIT  // Use joint, application must propose leave
}
