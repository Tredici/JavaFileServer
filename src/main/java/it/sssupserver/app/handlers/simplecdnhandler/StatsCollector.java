package it.sssupserver.app.handlers.simplecdnhandler;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.gson.GsonBuilder;

import it.sssupserver.app.handlers.simplecdnhandler.gson.FileStatsGson;
import it.sssupserver.app.handlers.simplecdnhandler.gson.StatsCollectorGson;

/**
 * This class will be charged of collecting and supplying
 * all all statistics regarding client operations affecting
 * this node
 */
public class StatsCollector {

    // per file stats
    public class FileStats {
        private String path;
        // how many redirect performed
        private AtomicLong redirects = new AtomicLong();
        // supplied how many times
        private AtomicLong supplied = new AtomicLong();
        // errors, include not found
        private AtomicLong errors = new AtomicLong();

        public FileStats(String path) {
            this.path = path;
        }

        public String getPath() {
            return path;
        }

        public long incRedirects() {
            return redirects.incrementAndGet();
        }

        public long getRedirects() {
            return redirects.get();
        }

        public long incSupplied() {
            return supplied.incrementAndGet();
        }

        public long getSupplied() {
            return supplied.get();
        }

        public long incErrors() {
            return errors.incrementAndGet();
        }

        public long getErrors() {
            return errors.get();
        }
    }

    private ConcurrentSkipListMap<String, FileStats> fileStats = new ConcurrentSkipListMap<>();

    public StatsCollector() {
    }

    public FileStats getFileStats(String path) {
        // take (or create) and return
        return fileStats.computeIfAbsent(path, p -> new FileStats(p));
    }

    public FileStats[] getFileStats() {
        return fileStats.values().toArray(FileStats[]::new);
    }

    public String asJson(boolean prettyPrinting) {
        var gBuilder = new GsonBuilder();
        if (prettyPrinting) {
            gBuilder = gBuilder.setPrettyPrinting();
        }
        var gson = gBuilder
            .registerTypeAdapter(FileStats.class, new FileStatsGson())
            .registerTypeAdapter(StatsCollector.class, new StatsCollectorGson())
            .create();
        var json = gson.toJson(getFileStats());
        return json;
    }

    public String asJson() {
        return asJson(false);
    }
}
