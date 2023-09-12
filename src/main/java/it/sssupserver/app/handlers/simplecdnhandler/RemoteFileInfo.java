
package it.sssupserver.app.handlers.simplecdnhandler;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import it.sssupserver.app.base.Path;

/**
 * Simplified version of LocalFileInfo used to maintain
 * references to file hold by remote nodes
 *
 * A PeerWatcher query the files held by a remote node and
 * then supply the results to the handler via this structure
 */
public class RemoteFileInfo {

    /**
     * Path used to identify the path on nodes
     */
    private Path searchPath;
    private Version bestVersion;

    public Path getSearchPath() {
        return searchPath;
    }
    public Version getBestVersion() {
        return bestVersion;
    }

    /**
     * Two idem reports info about two (possibly different)
     * versions of the same file.
     */
    public boolean refersToSameFile(RemoteFileInfo other) {
        return searchPath.equals(other.searchPath);
    }

    /**
     * Internally used for merge operation
     */
    private RemoteFileInfo(Path searchPath) {
        this.searchPath = searchPath;
    }

    /**
     * DO NOT MODIFY THIS INSTANCE!
     * Return a new instance describing the best (newest)
     * version of the file
     */
    public RemoteFileInfo merge(RemoteFileInfo other) {
        if (!refersToSameFile(other)) {
            throw new RuntimeException("Cannot merge RemoteFileInfo "
                + "referring to two different files");
        }
        var ans = new RemoteFileInfo(getSearchPath());

        var cmpV = getBestVersion().compareTo(other.getBestVersion());
        if (cmpV == 0) {
            // two possible suppliers
            var c = getBestVersion().clone();
            // merge sources
            c.candidateSupplier.putAll(other.getBestVersion().candidateSupplier);
            ans.bestVersion = c;
        } else {
            // copy best version
            ans.bestVersion = cmpV < 0 ?
                getBestVersion() : other.getBestVersion();
        }

        return ans;
    }

    /**
     * For a given file multiple versions myght be available
     */
    public static class Version implements Comparable<Version>, Cloneable {
        /**
         * When was the file created?
         */
        private Instant timestamp;

        /**
         * Original size of the file
         */
        private long size;

        /**
         * Was the file deleted?
         */
        private boolean deleted;

        /**
         * Information about the hash of the file
         */
        private String hashAlgorithm;
        private byte[] fileHash;

        /**
         * Obtain a clone of the other item
         */
        @Override
        public Version clone() {
            var ans = new Version(timestamp, size, hashAlgorithm, fileHash, deleted);
            ans.candidateSupplier = new ConcurrentSkipListMap<>(this.candidateSupplier);
            return ans;
        }

        public Instant getTimestamp() {
            return timestamp;
        }

        public long getSize() {
            return size;
        }

        public boolean isDeleted() {
            return deleted;
        }

        public String getHashAlgorithm() {
            return hashAlgorithm;
        }

        public byte[] getFileHash() {
            return fileHash;
        }

        /**
         * List of nodes from which file download can be tried.
         */
        private ConcurrentMap<Long, DataNodeDescriptor> candidateSupplier = new ConcurrentSkipListMap<>();

        private Version(
            Instant timestamp,
            long size,
            String hashAlgorithm,
            byte[] fileHash,
            boolean deleted
        ) {
            this.timestamp = timestamp;
            this.size = size;
            this.hashAlgorithm = hashAlgorithm;
            this.fileHash = fileHash;
            this.deleted = deleted;
        }

        public boolean equals(Object o) {
            if (o == null || !(o instanceof RemoteFileInfo.Version)) {
                return false;
            }
            var other = (RemoteFileInfo.Version)o;
            return getTimestamp().equals(other.getTimestamp())
                && isDeleted() == other.isDeleted()
                && getHashAlgorithm().equals(other.getHashAlgorithm())
                && Arrays.equals(getFileHash(), other.getFileHash());
        }

        /**
         * Compare two different versions
         */
        @Override
        public int compareTo(Version other) {
            if (other == null) {
                throw new NullPointerException("RemoteFileInfo.Version.compare");
            }
            var cmpTS = getTimestamp().compareTo(other.getTimestamp());
            if (cmpTS == 0) {
                // timestamp equals, must add check other parameters
                if (isDeleted()) {
                    // if both deleted they are equals
                    if (other.isDeleted()) {
                        return 0;
                    } else {
                        return 1;
                    }
                } else if (other.isDeleted()) {
                    // deleted appears always as older
                    return -1;
                } else {
                    // tie-break rules
                    // prefer shorter hash names
                    var cmpHA = getHashAlgorithm().compareTo(other.getHashAlgorithm());
                    if (cmpHA != 0) {
                        return cmpHA;
                    } else {
                        // compare arrays
                        return Arrays.compare(getFileHash(), other.getFileHash());
                    }
                }
            } else {
                return cmpTS;
            }
        }
    }

    public String getETag() {
        var v = getBestVersion();
        var eTag = SimpleCDNHandler.generateHttpETagHeader(v.getTimestamp(), v.getSize(), v.getHashAlgorithm(), v.getFileHash());
        return eTag;
    }

    public RemoteFileInfo addCandidateSupplier(DataNodeDescriptor node) {
        this.bestVersion.candidateSupplier.put(node.getId(), node);
        return this;
    }

    public List<DataNodeDescriptor> getCandidateSuppliers() {
        return this.getBestVersion().candidateSupplier.values().stream().collect(Collectors.toList());
    }

    public RemoteFileInfo(String searchPath, long size,
        Instant timestamp, String hashAlgorithm, byte[] hash,
        boolean deleted) {
        this.searchPath = new Path(searchPath);
        // assert filename match constraints
        if (FilenameCheckers.isValidPathName(this.searchPath)) {
            throw new RuntimeException("Invalid path: " + searchPath);
        }
        this.bestVersion = new Version(timestamp, size, hashAlgorithm, hash, deleted);
    }

    /**
     * Return a representation of the metadata associated with
     * the remote file
     */
    public FilenameMetadata getAssociatedMetadata() {
        var v = getBestVersion();
        var ans = new FilenameMetadata(this.searchPath.getBasename(), v.getTimestamp(), false, false, v.isDeleted());
        return ans;
    }

    /**
     * Get effective path, i.e. the one including metadata
     */
    public Path getEffectivePath() {
        var m = getAssociatedMetadata();
        var p = getSearchPath();
        var ans = p.getDirname().createSubfile(m.toString());
        return ans;
    }

    /**
     * Path for download, i.e. @tmp
     */
    public Path getDownloadPath() {
        var m = getAssociatedMetadata();
        m.setTemporary(true);
        var p = getSearchPath();
        var ans = p.getDirname().createSubfile(m.toString());
        return ans;
    }

}