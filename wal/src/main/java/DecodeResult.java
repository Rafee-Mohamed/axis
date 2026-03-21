import java.nio.ByteBuffer;

public sealed interface DecodeResult {
    record Record(ByteBuffer payload) implements DecodeResult {}
    record EndOfLog(long position, int crc) implements DecodeResult {}  // hit zeros
    record EndOfSegment(int crc) implements DecodeResult {}  // hit valid footer
    record Corrupt(long position, int crc) implements DecodeResult {}
    record Closed() implements DecodeResult {}
}
