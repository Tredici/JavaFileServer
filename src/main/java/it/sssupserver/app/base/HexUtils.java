package it.sssupserver.app.base;

import java.util.function.Predicate;
import java.util.regex.Pattern;

public class HexUtils {

    // Code source:
    //  https://stackoverflow.com/questions/9655181/java-convert-a-byte-array-to-a-hex-string
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toLowerCase().toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    private static Predicate<String> hexPatternTest;
    static {
        hexPatternTest = Pattern.compile(
            "^[a-fA-F0-9]*$"
        ).asMatchPredicate();
    }
    public static byte[] hexToBytes(String hex) {
        hex = hex.toLowerCase();
        if (hex.length() % 2 != 0 || !hexPatternTest.test(hex)) {
            throw new IllegalArgumentException("Invalid hex string: " + hex);
        }
        var ans = new byte[hex.length() >> 1];
        for (var i=0; i != ans.length; ++i) {
            var index1 = (i << 1);
            var index2 = index1 + 1;
            // first byte
            var c = hex.charAt(index1);
            ans[i] = (byte)((
                ('0' <= c && c <= '9') ?
                    c - '0' :
                    10 + (c - 'a')
            ) << 4);
            // second byte
            c = hex.charAt(index2);
            ans[i] |= (byte)(
                ('0' <= c && c <= '9') ?
                    c - '0' :
                    10 + (c - 'a')
            );
        }
        return ans;
    }

}
