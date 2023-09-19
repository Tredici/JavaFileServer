package it.sssupserver.app.handlers.simplecdnhandler.pathhashers;

public class DefaultPathHasher implements PathHasher {
    @Override
    public long hashFilename(String path) {
        return (long)path.hashCode();
    }    
}
