package it.sssupserver.app.handlers.simplecdnhandler.pathhashers;

/**
 * Interface hiding details of path to hash (file id) mapping.
 * All nodes in the topology should use the same hasher.
 */
public interface PathHasher {
    public long hashFilename(String path);
}
