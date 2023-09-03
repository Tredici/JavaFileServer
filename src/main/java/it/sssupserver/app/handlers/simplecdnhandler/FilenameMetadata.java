package it.sssupserver.app.handlers.simplecdnhandler;

import java.time.Instant;
import java.util.regex.Pattern;

import static it.sssupserver.app.handlers.simplecdnhandler.FilenameCheckers.*;

// Helper class used to parse true path names in order to
// extract
public class FilenameMetadata {
    // Filename structure:
    //  "{regular file name character}
    //   (@\d: timestamp)?
    //   (@corrupted)?
    //   (@tmp)?"

    // pattern used to insert metadata informations inside file names
    private static Pattern fileMetadataPattern;
    static {
        fileMetadataPattern = Pattern.compile("^(?<simpleName>[^@]*)(@(?<timestamp>\\d+))?(?<temporary>@tmp)?(?<deleted>@deleted)?(?<corrupted>@corrupted)?$");
    }

    private String simpleName;
    private Instant timestamp;
    private boolean corrupted;
    private boolean temporary;
    private boolean deleted;

    public FilenameMetadata(String filename) {
        // regex and parse
        var m = fileMetadataPattern.matcher(filename);
        if (!m.find()) {
            throw new RuntimeException("No pattern (" + fileMetadataPattern.pattern()
                + ") recognised in filename: " + filename);
        }
        simpleName = m.group("simpleName");
        var time = m.group("timestamp");
        var corr = m.group("corrupted");
        var tmp = m.group("temporary");
        var deleted = m.group("deleted");
        if (time != null) {
            this.timestamp = Instant.ofEpochMilli(Long.parseLong(time));
        }
        if (corr != null) {
            this.corrupted = true;
        }
        if (tmp != null) {
            this.temporary = true;
        }
        if (deleted != null) {
            this.deleted = true;
        }
    }

    public FilenameMetadata(
        String simpleName,
        Instant timestamp,
        boolean corrupted,
        boolean temporary,
        boolean deleted
    ) {
        if (!isValidRegularFileName(simpleName)) {
            throw new RuntimeException("Invalid filename: " + simpleName);
        }
        this.simpleName = simpleName;
        this.timestamp = timestamp;
        this.corrupted = corrupted;
        this.temporary = temporary;
        this.deleted = deleted;
    }

    /**
     * Return string before first '@'
     * @return
     */
    public String getSimpleName() {
        return simpleName;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setCorrupted(boolean corrupted) {
        this.corrupted = corrupted;
    }

    public boolean isCorrupted() {
        return corrupted;
    }

    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
    }

    public boolean isTemporary() {
        return temporary;
    }

    public void setDeteleted(boolean deleted) {
        this.deleted = deleted;
    }

    public boolean isDeteleted() {
        return deleted;
    }

    // used to test if any data was extracted from the file name
    public boolean holdMetadata() {
        return timestamp != null || corrupted || temporary || deleted;
    }

    public String toString() {
        var sb = new StringBuffer();
        sb.append(simpleName);
        if (timestamp != null) {
            sb.append('@').append(timestamp.toEpochMilli());
        }
        if (isTemporary()) {
            sb.append("@tmp");
        }
        if (isDeteleted()) {
            sb.append("@deleted");
        }
        if (isCorrupted()) {
            sb.append("@corrupted");
        }
        return sb.toString();
    }
}
