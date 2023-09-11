package it.sssupserver.app.handlers.simplecdnhandler;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import it.sssupserver.app.handlers.simplecdnhandler.DataNodeDescriptor.Status;

/**
 * Simple class used to track a possible peer and monitor keepalives.
 * we have to look for
 *
 * PeerWatcher cross the following states:
 *  - Initialization
 *  - Discovery
 *  - KeepAlive
 *  - Shutdown
 *
 * By default, a PeerWatcher handle remote node discover,
 * i.e. it POST /api/discovery
 */
/**
 * Used to handle PeerWatcher protocol, it is a inner class
 * because it relies on many components provided by the
 * SimpleCDNHandler class:
 *  - access thread pools to scheduler different operations
 */
public class PeerWatcher implements Runnable, BiConsumer<HttpResponse<String>,Throwable> {
    // Discovery scheduling paramenter
    public static final long SCHEDULING_DELAY = 3;
    public static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    // KeepAlive parameter
    public static final long KEEP_ALIVE_DELAY = 3;
    // TopologyWatcher parameter
    public static final long TOPOLOGY_DELAY = 3;
    // FileWatcher parameter
    public static final long FILE_WATCHER_DELAY = 1;

    // After ERROR_LIMIT consecutive errors, trigger deletion
    public static final int ERROR_LIMIT = 3;

    // Url to periodically check for peer risponse
    private URL url;
    private URI remoteUri;
    // How many consecutive errors have been found till now?
    private int errorCounter = 0;
    // when did last error occurred?
    private Instant lastError;

    private SimpleCDNHandler handler;

    private ScheduledExecutorService getTimedThreadPool() {
        return handler.getTimedThreadPool();
    }

    private ExecutorService getThreadPool() {
        return handler.getThreadPool();
    }

    // used for keepalive
    private PeerKeepAliver peerKeepAliver = new PeerKeepAliver();
    // used to get topology updates
    private TopologyWatcher peerTopologyWatcher = new TopologyWatcher();
    // used to get file updates
    private FileWatcher peerFileWatcher = new FileWatcher();

    // Only SimpleCDN handler can instantiate new watchers
    PeerWatcher(SimpleCDNHandler handler, URL remoteUrl) throws URISyntaxException {
        this.handler = handler;
        this.url = remoteUrl;
        this.remoteUri = remoteUrl.toURI();
    }

    public URL getUrl() {
        return url;
    }

    public boolean started = false;
    // is Discovery protocol started?
    public boolean isStarted() {
        return started;
    }

    /**
     * Start discovery
     */
    public void start() {
        if (!isStarted()) {
            started = true;
            schedule();
        }
    }

    /**
     * Stop discovery protocol
     */
    public void stop() {
        if (isStarted()) {
            // stop also keepalive
            peerKeepAliver.stop();
            started = false;
            schedule.cancel(false);
            schedule = null;
        }
    }

    private ScheduledFuture<?> schedule;

    /**
     * Schedule POST /api/topology to peer,
     * do nothing if it has already been scheduled
     */
    public synchronized void schedule(long delay) {
        if (isStarted() || schedule == null || schedule.isDone()) {
            schedule = getTimedThreadPool()
            .schedule(this, delay, TIME_UNIT);
        }
    }

    // Schedule with default delay
    public void schedule() {
        schedule(SCHEDULING_DELAY);
    }

    /**
     * Scheduled on the timedThreadPool to handle
     * POST /api/topology
     *
     * HTTP response is handled via threadPool
     */
    @Override
    public void run() {
        var req = handler.buildApiTopologyPOST(remoteUri);
        // send http request
        handler.getHttpClient().sendAsync(req, BodyHandlers.ofString(StandardCharsets.UTF_8))
        .whenCompleteAsync(this, getThreadPool());
    }


    private void onError() {
        // error, simply reschedule
        ++errorCounter;
        lastError = Instant.now();
        schedule();
    }

    /**
     * Handle HTTP response, or errors.
     *
     * In case of good response:
     *  - two possible outcome:
     *      1. response contains an Identity
     *          - check if Identity is compatible with local
     *            node, in such case:
     *              - bind watcher with the corresponding
     *              identy
     *          - supply list of found elements to hanlder
     *            (that will eventually start discovery)
     *              - handler will possibly
     *      2. response does not contain an Identity
     * In case of bad response:
     *  -
     */
    @Override
    public void accept(HttpResponse<String> res, Throwable err) {
        if (err != null || (res != null && res.statusCode() != 200)) {
            onError();
        } else {
            // parse body
            try {
                var json = res.body();
                var data = TopologyWithIdentityGson.parse(json);
                // check if identity available
                if (data.getIdentity() != null && handler.isDataNodeCompatible(data.getIdentity())) {
                    // identity received, can associate, inform handler
                    if (!handler.associatePeerWithWatcher(data.getIdentity(), this)) {
                        schedule();
                    }
                } else {
                    // no available identity, simply reschedule for possible updates
                    schedule();
                }
                // report list of possible new peers
                handler.possiblePeers(data.getSnapshot());
                // reset error counter
                errorCounter = 0;
            } catch (Exception e) {
                onError();
            }
        }
    }

    /**
     * Peer the watcher is charged to monitor
     */
    private DataNodeDescriptor associatedPeer;

    public DataNodeDescriptor getAssociatedPeer() {
        return associatedPeer;
    }

    private void onStatusUpdate() {
        if (associatedPeer.getStatus() == Status.STOPPING) {
            // TODO: handle transition to STOPPING
        }
    }
    private void onTopologyUpdate() {
        // simply schedule a topology check for possible resync
        peerTopologyWatcher.schedule();
    }
    private void onFileUpdate() {
        // TODO: schedule file index check.
        //
        // should notify handler and check all files
        // held by the node in order to start downloading
        // the ones that this node will be cherged of
        // supplying
        peerFileWatcher.schedule();
    }

    /**
     * Check all possible status updates, detected via
     * timestamp updates, on the remote node.
     * Might also be called by the handler itself on reception
     * of POST /api/* requests containing source identity.
     */
    public synchronized void checkForStatusUpdate(DataNodeDescriptor info) {
        var ap = associatedPeer;
        if (ap == null) {
            // 
            return;
        }
        // by semplicity
        if (ap.describeSameInstance(info)) {
            // check status update
            if (ap.availableStatusUpdate(info)) {
                onStatusUpdate();
            }
            // check topology update
            if (ap.availableTopologyUpdate(info)) {
                onTopologyUpdate();
            }
            // check files updates
            if (ap.availableFileUpdate(info)) {
                onFileUpdate();
            }
        }
    }

    /**
     * Associate this node with given descriptor,
     * then start keepalive.
     * THIS METHOD IS INVOKED BY THE HANDLER!
     */
    public void bind(DataNodeDescriptor descriptor) {
        associatedPeer = descriptor;
        peerKeepAliver.start();
    }
    
    public DataNodeDescriptor unbind() {
        var ans = associatedPeer;
        associatedPeer = null;
        return ans;
    }

    /**
     * Special class specifically dedicated to periodally
     * pinging the status of the remote node itself.
     * POST /api/hello
     */
    private class PeerKeepAliver implements Runnable, BiConsumer<HttpResponse<String>,Throwable> {

        // error counter
        private int errorCounter = 0;
        private Instant lastError;

        private void onError() {
            ++errorCounter;
            lastError = Instant.now();
            if (errorCounter == ERROR_LIMIT) {
                // TODO: consider peer FAILED or SHUTDOWN
                switch (associatedPeer.getStatus()) {
                    case STOPPING:
                        // in case it was stopping probably it has been shutdown
                        associatedPeer.setStatus(DataNodeDescriptor.Status.SHUTDOWN);
                        // if node is shutdown, timestamp cannot by received! It must
                        // be set manually here
                        associatedPeer.setLastStatusChange(Instant.now());
                        break;
                    default:
                        // otherwise it is probably failed
                        associatedPeer.setStatus(DataNodeDescriptor.Status.FAILED);
                        // failure is an anomalous outcome, associated
                        // timestamp can only be set by the local node
                        associatedPeer.setLastStatusChange(Instant.now());
                        break;
                }
                // stop keepalive
                stop();
                // back to discovery
                PeerWatcher.this.schedule();
                // inform wrapper
                handler.decouplePeerFromWatcher(associatedPeer, PeerWatcher.this);
            }
        }

        public boolean started = false;
        // is KeepAlive protocol started?
        public boolean isStarted() {
            return started;
        }

        /**
         * Start keepalive
         */
        public void start() {
            if (!isStarted()) {
                started = true;
                schedule();
            }
        }

        /**
         * Stop keepalive protocol
         */
        public void stop() {
            if (isStarted()) {
                started = false;
                schedule.cancel(false);
                schedule = null;
            }
        }

        private ScheduledFuture<?> schedule;

        /**
         * Schedule a new POST /api/identity
         */
        private synchronized void schedule(long delay) {
            if (isStarted() || schedule == null || schedule.isDone()) {
                schedule = getTimedThreadPool()
                .schedule(this, delay, TIME_UNIT);
            }
        }

        private void schedule() {
            schedule(KEEP_ALIVE_DELAY);
        }

        /**
         * Receive response to
         * POST /api/hello
         */
        @Override
        public void accept(HttpResponse<String> res, Throwable err) {
            if (err != null || (res != null && res.statusCode() != 200)) {
                onError();
            } else {
                var ap = associatedPeer;
                if (ap == null) {
                    // prevent race condition
                    return;
                }
                try {
                    var json = res.body();
                    var identity = DataNodeDescriptor.fromJson(json);
                    // check if the response come from the node we know
                    if (!ap.describeSameInstance(identity)) {
                        // decouple from current peer
                        handler.decouplePeerFromWatcher(ap, PeerWatcher.this);
                        // create new association
                        handler.associatePeerWithWatcher(identity, PeerWatcher.this);
                    } else {
                        // should check all possible status updates
                        PeerWatcher.this.checkForStatusUpdate(identity);
                    }
                    // no error
                    errorCounter = 0;
                } catch (Exception e) {
                    onError();
                }
            }
        }

        /**
         * Scheduled on timedThreadPool
         * POST /api/hello
         */
        @Override
        public void run() {
            var ap = associatedPeer;
            if (ap == null) {
                // prevent race condition
                return;
            }
            try {
                var rUri = ap.getRandomManagementEndpointURL().toURI();
                var req = handler.buildApiHelloPOST(rUri);
                // send http request
                handler.getHttpClient().sendAsync(req, BodyHandlers.ofString(StandardCharsets.UTF_8))
                .whenCompleteAsync(this, getThreadPool());                
            } catch (Exception e) {
                onError();
            }
        }
    }

    /**
     * Special class dedicated to discover topology update.
     * The main difference with the outer class is that
     */
    private class TopologyWatcher implements Runnable, BiConsumer<HttpResponse<String>,Throwable> {

        // How many consecutive errors have been found till now?
        // when did last error occurred?
        private AtomicInteger errorCounter = new AtomicInteger();
        private Instant lastError;

        private void onError() {
            lastError = Instant.now();
            if (errorCounter.incrementAndGet() < ERROR_LIMIT) {
                reschedule();
            }
        }

        @Override
        public void accept(HttpResponse<String> res, Throwable err) {
            if (err != null || (res != null && res.statusCode() != 200)) {
                onError();
            } else {
                // parse body
                try {
                    var json = res.body();
                    var data = TopologyWithIdentityGson.parse(json);
                    // check if identity available
                    if (data.getIdentity() != null && handler.isDataNodeCompatible(data.getIdentity())) {
                        // report list of possible new peers
                        handler.possiblePeers(data.getSnapshot());                        
                        // reset error counter
                        errorCounter.set(0);
                    }
                } catch (Exception e) {
                    onError();
                }
            }
        }

        /**
         * Send POST /api/topology
         */
        @Override
        public void run() {
            var ap = associatedPeer;
            if (ap == null) {
                // prevent race condition
                return;
            }
            try {
                var rUri = ap.getRandomManagementEndpointURL().toURI();
                var req = handler.buildApiHelloPOST(rUri);
                // send http request
                handler.getHttpClient().sendAsync(req, BodyHandlers.ofString(StandardCharsets.UTF_8))
                .whenCompleteAsync(this, getThreadPool());                
            } catch (Exception e) {
                onError();
            }
        }

        private ScheduledFuture<?> schedule;
        public void schedule(long delay) {
            errorCounter.set(0);
            reschedule(delay);
        }

        public void schedule() {
            schedule(TOPOLOGY_DELAY);
        }

        private synchronized void reschedule(long delay) {
            if (schedule == null || schedule.isDone()) {
                schedule = getTimedThreadPool()
                .schedule(this, delay, TIME_UNIT);
            }
        }

        private void reschedule() {
            reschedule(TOPOLOGY_DELAY);
        }
    }

    /**
     * Auxiliary class used to query the remote endpoint
     * GET /api/suppliables
     */
    private class FileWatcher implements Runnable, BiConsumer<HttpResponse<String>,Throwable> {

        private AtomicInteger errorCounter = new AtomicInteger();
        private Instant lastError;

        private void onError() {
            lastError = Instant.now();
            if (errorCounter.incrementAndGet() < ERROR_LIMIT) {
                reschedule();
            }
        }

        private ScheduledFuture<?> schedule;

        /**
         * Immediately start a file check
         */
        public synchronized void schedule() {
            if (schedule != null && !schedule.isDone()) {
                // maybe pending, stop wait and trigger immediately
                schedule.cancel(false);
            }
            // reset error count
            errorCounter.set(0);
            getThreadPool().execute(this);
        }

        /**
         * Called internally in case of errors
         */
        private synchronized void reschedule() {
            // do nothing if already queued
            if (schedule == null || schedule.isDone() || schedule.isCancelled()) {
                // otherwise schedule
                schedule = getTimedThreadPool()
                .schedule(this, FILE_WATCHER_DELAY, TIME_UNIT);
            }
        }

        /**
         * GET /api/suppliables response
         */
        @Override
        public void accept(HttpResponse<String> res, Throwable err) {
            if (err != null || (res != null && res.statusCode() != 200)) {
                onError();
            } else {
                var ap = associatedPeer;
                if (ap == null) {
                    // prevent race condition
                    return;
                }
                try {
                    var json = res.body();
                    var remotes = RemoteFilesWithIdentityGson.parse(json);
                    if (remotes.getIdentity() != null && ap.describeSameInstance(remotes.getIdentity())) {
                        // only if valid identity check content
                        var files = remotes.getFiles();
                        handler.possibleFiles(ap, files);
                        // nothing more to do, it will be the handler
                        // charged of handling downloads
                    }
                    // no error
                    errorCounter.set(0);
                } catch (Exception e) {
                    onError();
                }
            }
        }

        /**
         * GET /api/suppliables request
         */
        @Override
        public void run() {
            var ap = associatedPeer;
            if (ap == null) {
                // prevent race condition
                return;
            }
            try {
                var rUri = ap.getRandomManagementEndpointURL().toURI();
                var req = handler.buildApiSuppliablesGET(rUri);
                // send http request
                handler.getHttpClient().sendAsync(req, BodyHandlers.ofString(StandardCharsets.UTF_8))
                .whenCompleteAsync(this, getThreadPool());
            } catch (Exception e) {
                onError();
            }
        }

    }
}
