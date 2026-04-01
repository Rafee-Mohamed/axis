package io.disys.axis.storage.swmr;

public class LexigographicPackedByteComparator implements PackedByteComparator {
    @Override
    public int compare(byte[] bytes, int start, int end, byte[] key) {
        var len1 = end - start;
        var len2 = key.length;

        var minLen = Math.min(len1, len2);

        for (var i = 0; i < minLen; i++) {
            // Java bytes are signed (-128 to 127). We convert them to unsigned (0 to 255)
            // using & 0xFF so that comparison is lexicographically correct for raw byte data.
            // byte b = -1; -> -1 < 1      // actually 255 in unsigned, result in wrong comparison
            // int x = b & 0xFF; → 255 > 1 // correct comparison
            var b1 = bytes[start + i] & 0xFF;
            var b2 = key[i] & 0xFF;

            if (b1 != b2) {
                return b1 - b2;
            }
        }

        return len1 - len2;
    }
}
