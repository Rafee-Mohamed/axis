package consensus.config;

public interface PeerInflightConfig {
    int maxInflightMsgs();
    long maxInflightBytes();
}
