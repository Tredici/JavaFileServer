package it.sssupserver.app.handlers.simplecdnhandler;

import java.time.Instant;
import java.util.regex.Pattern;

import it.sssupserver.app.commands.utils.FileReducerCommand;
import static it.sssupserver.app.base.HexUtils.*;

public class ETagParser {
    private static Pattern eTagPattern;
    static {
        var hashes = FileReducerCommand.getAvailableHashAlgorithms();
        var regex = new StringBuilder(512)
            .append('^')
            // timestamp
            .append("(?<timestamp>\\d+)")
            .append(':')
            // size
            .append("(?<size>\\d+)")
            .append(':')
            // hash algorithm
            .append("(?<algorithm>(")
            .append(

                String.join("|", hashes)
            )
            .append("))")
            .append(':')
            // hash
            .append(
                "(?<hash>[0-9A-F])"
            )
            .append('$')
            .toString();
        eTagPattern = Pattern.compile(regex);
    }



    // used to test ETag
    private String eTag;

    private Instant timestamp;
    private long size;
    private String hashAlgorithm;
    private byte[] hash;

    public ETagParser(String s) {
        var m = eTagPattern.matcher(s);
        if (!m.find()) {
            throw new RuntimeException("No pattern (" + eTagPattern.pattern()
                + ") recognised in ETag: " + s);
        }
        eTag = s;
        var timestamp = m.group("timestamp");
        var size = m.group("size");
        var algorithm = m.group("algorithm");
        var hash = m.group("hash");
        if (timestamp == null ||
            size == null ||
            algorithm == null ||
            hash == null ) {
            if (!m.find()) {
                throw new RuntimeException("No pattern (" + eTagPattern.pattern()
                    + ") recognised in ETag: " + s);
            }
        }
        this.timestamp = Instant.ofEpochMilli(Long.parseLong(timestamp));
        this.size = Long.parseLong(size);
        this.hashAlgorithm = algorithm;
        this.hash = hexToBytes(hash);
    }

    public String geteTag() {
        return eTag;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public long getSize() {
        return size;
    }

    public String getHashAlgorithm() {
        return hashAlgorithm;
    }

    public byte[] getHash() {
        return hash;
    }

    @Override
    public String toString() {
        return eTag;
    }
}