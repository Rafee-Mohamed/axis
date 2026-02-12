package consensus.algorithm;

import consensus.membership.MembershipConfig;

/**
 * @param term - term of last compacted entry
 * @param index - last compacted entry index
 * @param membershipConfig - membership configuration during snapshot taken
 * @param data
 */
public record Snapshot (
        long term,
        long index,
        MembershipConfig membershipConfig,
        byte[] data
){
}
