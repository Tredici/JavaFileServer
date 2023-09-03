
package it.sssupserver.app.handlers.simplecdnhandler;

import java.time.Instant;

import it.sssupserver.app.base.Path;

/**
 * Simplified version of LocalFileInfo used to maintain
 * references to file hold by remote nodes
 */
public class RemoteFileInfo {

    /**
     * Path used to identify the path on nodes
     */
    private Path searchPath;


    /**
     * For a given file multiple versions myght be available
     */
    public class Version {
        /**
         * When was the file created?
         */
        private Instant timestamp;
    
        /**
         * Was the file deleted?
         */
        private boolean deleted;
    
        /**
         * Information about the hash of the file
         */
        private String hashAlgorithm;
        private byte[] fileHash;
        
    
        public Path getSearchPath() {
            return searchPath;
        }
    
        public Instant getTimestamp() {
            return timestamp;
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

    }

}