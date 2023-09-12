package it.sssupserver.app.handlers.simplecdnhandler;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.util.List;
import java.util.Map;

import it.sssupserver.app.base.BufferManager;
import it.sssupserver.app.base.FileTree;
import it.sssupserver.app.base.BufferManager.BufferWrapper;
import it.sssupserver.app.base.FileTree.Node;
import it.sssupserver.app.commands.utils.FileReducerCommand;
import it.sssupserver.app.commands.utils.FutureDeleteCommand;
import it.sssupserver.app.commands.utils.FutureFileSizeCommand;
import it.sssupserver.app.commands.utils.FutureMkdirCommand;
import it.sssupserver.app.commands.utils.FutureMoveCommand;
import it.sssupserver.app.commands.utils.ListTreeCommand;
import it.sssupserver.app.commands.utils.QueableCommand;
import it.sssupserver.app.commands.utils.QueableCreateCommand;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.handlers.RequestHandler;
import it.sssupserver.app.handlers.httphandler.HttpSchedulableReadCommand;
import it.sssupserver.app.handlers.simplecdnhandler.SimpleCDNConfiguration.HttpEndpoint;
import it.sssupserver.app.users.Identity;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpResponse.BodySubscriber;
import java.net.http.HttpResponse.ResponseInfo;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static it.sssupserver.app.handlers.httphandler.HttpResponseHelpers.*;
import static it.sssupserver.app.handlers.simplecdnhandler.FilenameCheckers.*;
import static it.sssupserver.app.base.HexUtils.*;

public class SimpleCDNHandler implements RequestHandler {

    // by default this is the name of the configuration file, contained inside the CWD
    public static final String DEFAULT_CONFIG_FILE = "SimpleCDN.json";
    private Path config_file = Path.of(DEFAULT_CONFIG_FILE);

    // running configuration loaded from comfig file inside constructor
    private SimpleCDNConfiguration config;
    // identity associated with current replica
    private Identity identity;

    // when was this node started
    private Instant startInstant;

    private Duration httpConnectionTimeout = Duration.ofSeconds(30);
    private Duration httpRequestTimeout = Duration.ofSeconds(30);
    private HttpClient httpClient;

    // hold informations about current instance
    DataNodeDescriptor thisnode;

    public DataNodeDescriptor getThisnode() {
        return thisnode;
    }

    /**
     * Prevent race condition:
     *  never decrease timestamp
     */
    private Object topUpdate = new Object(); // do not share locks among independed function
    public Instant updateLastTopologyUpdateTimestamp() {
        var now = Instant.now();
        synchronized(topUpdate) {
            if (thisnode.getLastTopologyUpdate().isBefore(now)) {
                thisnode.setLastTopologyUpdate(now);
            }
        }
        return now;
    }

    private Object fileUpdate = new Object();
    public Instant updateLastFileUpdateTimestamp() {
        var now = Instant.now();
        synchronized(fileUpdate) {
            if (thisnode.getLastFileUpdate().isBefore(now)) {
                thisnode.setLastFileUpdate(startInstant);
            }
        }
        return now;
    }

    /**
     * Can this node interact with the other?
     */
    public boolean isDataNodeCompatible(DataNodeDescriptor o) {
        return o.getDataendpoints().length > 0
         && o.getManagerendpoint().length > 0
         && thisnode.areComplatible(o);
    }

    public SimpleCDNConfiguration getConfig() {
        return config;
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }

    private String thisnodeAsJson(boolean prettyPrinting) {
        var gBuilder = new GsonBuilder();
        if (prettyPrinting) {
            gBuilder = gBuilder.setPrettyPrinting();
        }
        var gson = gBuilder
            .registerTypeAdapter(DataNodeDescriptor.class, new DataNodeDescriptorGson())
            .create();
        var json = gson.toJson(thisnode);
        return json;
    }

    private String thisnodeAsJson() {
        return thisnodeAsJson(false);
    }

    private URL[] getClientEndpoints() {
        return thisnode.dataendpoints;
    }

    private URL[] getManagementEndpoints() {
        return thisnode.managerendpoint;
    }

    // hold topology seen by this node
    private Topology topology;
    /**
     * Directly operate on the current node
     * @param R
     * @return
     */
    public List<DataNodeDescriptor> getNeighboours() {
        return topology.getNeighboours(thisnode, thisnode.getReplicationFactor());
    }

    // find previous of the current node,
    // return null if current node is alone
    // (i.e. it is its own successor)
    public DataNodeDescriptor findPrevious() {
        // any previous?
        return topology.findPrevious(this.thisnode);
    }

    // find successor of the current node,
    // return null if current node is alone
    // (i.e. it is its own successor)
    public DataNodeDescriptor findSuccessor() {
        // any successor?
        return topology.findSuccessor(this.thisnode);
    }

    // respond with redirect message for supplied file
    // return true if current node own specified file
    // NOTE: test OWNERSHIP, not ability to supply it!
    // should not consider initial "/"
    public boolean testOwnershipOrRedirect(String path, HttpExchange exchange) {
        if (topology.isFileOwned(path)) {
            return true;
        } else {
            var owner = topology.getFileOwner(path);
            var index = (int)(owner.dataendpoints.length * Math.random());
            var redirect = owner.dataendpoints[index];
            try {
                // 308 Permanent Redirect
                //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/308
                exchange.sendResponseHeaders(404, 0);
                // Set Location header
                //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Location
                exchange.getResponseHeaders()
                    .add("Location", redirect.toURI().resolve(path).toString());
                exchange.getResponseBody().flush();
                exchange.close();
            } catch (Exception e) {
                // TODO: should only log error
            }
            return false;
        }
    }

    // test if this node is allowed to return specified file
    // should not consider initial "/"
    public boolean testSupplyabilityOrRedirect(String path, HttpExchange exchange) {
        if (topology.isFileSupplier(path)) {
            return true;
        } else {
            var owner = topology.peekRandomSupplier(path);
            var index = (int)(owner.dataendpoints.length * Math.random());
            var redirect = owner.dataendpoints[index];
            try {
                exchange.sendResponseHeaders(404, 0);
                exchange.getResponseHeaders()
                    .add("Location", redirect.toURI().resolve(path).toString());
                exchange.getResponseBody().flush();
                exchange.close();
            } catch (Exception e) {
                // TODO: should only log error
            }
            return false;
        }
    }

    public boolean testOwnershipOrRedirectToManagement(String path, HttpExchange exchange) {
        if (topology.isFileOwned(path)) {
            return true;
        } else {
            var owner = topology.getFileOwner(path);
            var index = (int)(owner.managerendpoint.length * Math.random());
            var redirect = owner.managerendpoint[index];
            try {
                // 308 Permanent Redirect
                //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/308
                exchange.sendResponseHeaders(404, 0);
                // Set Location header
                //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Location
                exchange.getResponseHeaders()
                    .add("Location", redirect.toURI().resolve(path).toString());
                exchange.getResponseBody().flush();
                exchange.close();
            } catch (Exception e) {
                // TODO: should only log error
            }
            return false;
        }
    }

    public boolean testSupplyabilityOrRedirectToManagement(String path, HttpExchange exchange) {
        if (topology.isFileSupplier(path)) {
            return true;
        } else if (path.contains("@")) {
            // if path contains metadata disable redirection
            return true;
        } else {
            var owner = topology.peekRandomSupplier(path);
            var index = (int)(owner.managerendpoint.length * Math.random());
            var redirect = owner.managerendpoint[index];
            try {
                exchange.sendResponseHeaders(404, 0);
                exchange.getResponseHeaders()
                    .add("Location", redirect.toURI().resolve(path).toString());
                exchange.getResponseBody().flush();
                exchange.close();
            } catch (Exception e) {
                // TODO: should only log error
            }
            return false;
        }
    }

    /**
     * Called when an update is received from a node already
     * known
     */
    public void handleRemoteNodeUpdate(DataNodeDescriptor node) {
        var ts = Instant.now();
    }

    /**
     * To be called when the node with the specified id
     * leave the topology.
     */
    public void handleRemoteNodeExit(long id) {
        // remove node from topology
        // check R-1 predecessor for resync
        // TODO: complete
    }

    /**
     * This class maintains a view of the files
     * managed by this DataNode.
     * It should be re-initialized every time this
     * handler is started.
     */
    public class ManagedFileSystemStatus {
        // hash algorithm chosen to compare files owned by replicas
        public static final String HASH_ALGORITHM = FileReducerCommand.MD5;

        // Maintain a view of the file tree managed by this replica node
        FileTree snapshot;

        // add new file node
        public FileTree.Node addRegularFileNode(it.sssupserver.app.base.Path path) {
            return snapshot.addNode(path);
        }

        // putting restrictions on directory and file names
        // allow us to

        /**
         * Used to caracterize a locally holded file.
         *
         * Maintains a description for register of locally owned nodes
         * This is used to handle multiple file versions
         */
        public class LocalFileInfo {
            // a file is identified by its path as String
            // HTTP requests hold this identifier
            private String searchPath;

            public LocalFileInfo(String searchPath) {
                this.searchPath = searchPath;
            }

            public String getSearchPath() {
                return searchPath;
            }

            public class Version {
                // time associated with
                private boolean corrupted = false;
                private boolean tmp = false;
                private boolean deleted;
                // associate file with Node olding its properties
                private FileTree.Node node;

                public Version(FileTree.Node node, boolean isCorrupted, boolean isTemporary, boolean isDeleted) throws Exception {
                    this.node = node;
                    this.corrupted = isCorrupted;
                    this.tmp = isTemporary;
                    this.deleted = isDeleted;
                    // if isDeleted should retrieve ETag from file content
                    // if isCorrupted ignore
                    if (isDeleted && !isCorrupted) {
                        // retrieve
                        var accumulator = new ArrayList<Byte>(512);
                        var reducer = (BiFunction<List<Byte>, ByteBuffer, List<Byte>>) (List<Byte> a, ByteBuffer b) -> {
                            while (b.hasRemaining()) {
                                a.add(b.get());
                            }
                            return a;
                        };
                        var finalizer = (Function<List<Byte>,String>) (List<Byte> a) -> {
                            var B = a.toArray(new Byte[0]);
                            var b = new byte[B.length];
                            // return first line
                            return new String(b, StandardCharsets.UTF_8).split("\n")[0];
                        };
                        var firstLine = FileReducerCommand.reduce(executor, getPath(), identity, accumulator, reducer, finalizer).toString();
                        // validate ETAG
                        try {
                            var eTag = new ETagParser(firstLine);
                            // set parameters to Node object
                            this.node.setLastModified(eTag.getTimestamp());
                            this.node.setSize(eTag.getSize());
                            this.node.setFileHash(eTag.getHashAlgorithm(), eTag.getHash());
                        } catch (Exception e) {
                            // if fail consider as corrupted
                            isCorrupted = true;
                        }
                        // extract info from etag
                    }

                    // add to list of versions
                    LocalFileInfo.this.versions.add(this);
                }

                // identifier of the container object
                public String getSearchPath() {
                    return searchPath;
                }

                public long getSize() {
                    return node.getSize();
                }

                public Instant getLastUpdateTimestamp() {
                    return node.getLastModified();
                }

                public String getHashAlgotrithm() {
                    return node.getHashAlgorithm();
                }

                public byte[] getFileHash() {
                    return node.getFileHash();
                }

                public  it.sssupserver.app.base.Path getPath() {
                    return node.getPath();
                }

                // corrupted files should not be returned
                public boolean isCorrupted() {
                    return corrupted;
                }

                // not completed file, to be
                public boolean isTmp() {
                    return tmp;
                }

                public boolean isDeleted() {
                    return deleted;
                }

                /**
                 * Mark current file version as to be deleted
                 * @throws Exception
                 */
                public void markAsDeleted() throws Exception {
                    // short circuit
                    if (isDeleted()) {
                        // Idempotent
                        return;
                    }
                    synchronized(this) {
                        if (isDeleted()) {
                            // Idempotent
                            return;
                        }
                        var currentPath = getPath();
                        /**
                         * Perform renaming:
                         *  cannot be done atomically,
                         *  create new "deleted version"
                         *  delete valid one
                         */
                        var metadata = getMetadata();
                        // should delete
                        metadata.setDeteleted(true);
                        var dstPath = currentPath.getDirname()
                            .createSubfile(metadata.toString());
                        {
                            // local etag
                            var etag = generateHttpETagHeader();
                            // get current timestamp
                            var deletetionTS = Instant.now();
                            // content of the file to be removed
                            var dc = new StringBuilder()
                                .append(etag)
                                .append('\n')
                                .append(deletetionTS.toEpochMilli())
                                .append('\n')
                                .toString();
                            // generate content of new file
                            var bytecontent = dc.getBytes(StandardCharsets.UTF_8);
                            // put content in a buffer
                            var w = BufferManager.getBuffer();
                            var buf = w.get();
                            buf.put(bytecontent);
                            buf.flip();
                            // schedule creation of file
                            var cc = QueableCreateCommand.submit(executor, dstPath, identity, w);
                            if (cc.getFuture().get() != true) {
                                throw new RuntimeException("Failed to delete file: " + searchPath);
                            }
                        }
                        // delete original one
                        var f = FutureDeleteCommand.delete(executor, currentPath, identity);
                        f.get();
                        // change reference in Node object
                        node.rename(dstPath);
                        this.deleted = true;
                    }
                }

                public FilenameMetadata getMetadata() {
                    var basename = getPath().getBasename();
                    return new FilenameMetadata(basename);
                }

                /**
                 * A file can be supplied to clients if:
                 *  it is not temporary
                 *  it is not corrupted
                 */
                public boolean isSuppliable() {
                    return !isCorrupted() && !isTmp();
                }

                public boolean matchMetadata(FilenameMetadata metadata) {
                    return isCorrupted() == metadata.isCorrupted()
                        && isTmp() == metadata.isTemporary()
                        && metadata.getTimestamp().equals(node.getLastModified());
                }

                /**
                 * Generate ETag header to be used in HTTP messages
                 *  https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag
                 *
                 * Tag is composed by:
                 *  timestamp:file_size:hash_algoritm:hash_string
                 */
                public String generateHttpETagHeader() {
                    return SimpleCDNHandler.generateHttpETagHeader(
                        node.getLastModified(),
                        node.getSize(),
                        node.getHashAlgorithm(),
                        node.getFileHash()
                        );
                }

                /**
                 * Compare local version with remote one
                 */
                public boolean isRemoteNewer(RemoteFileInfo remoteFile) {
                    // compare timestamps
                    if (remoteFile.getBestVersion().getTimestamp().isBefore(this.getLastUpdateTimestamp())) {
                        // remote is older, ignore
                        return false;
                    } else if (remoteFile.getBestVersion().getTimestamp().isAfter(this.getLastUpdateTimestamp())) {
                        // remote is newer, can download
                        return true;
                    }
                    // same age, compare deleted status
                    if (remoteFile.getBestVersion().isDeleted()) {
                        // if only remote deleted, update, otherwise do nothing
                        return !this.isDeleted();
                    }
                    // if only local is deleted ignore remote
                    if (this.isDeleted()) {
                        return false;
                    }
                    // prefer simpler hashing algorithm - same logic used for
                    // RemoteFileInfo.Version.compareTo
                    var cmpHA = remoteFile.getBestVersion().getHashAlgorithm()
                        .compareTo(this.getHashAlgotrithm());
                    if (cmpHA != 0) {
                        return cmpHA > 0;
                    }
                    // prefer greate hash value
                    return Arrays.compare(remoteFile.getBestVersion().getFileHash(), this.getFileHash()) > 0;
                }
            }

            public Version addVersion(FileTree.Node node, FilenameMetadata metadata) throws Exception {
                var ans = new Version(node, metadata.isCorrupted(), metadata.isTemporary(), metadata.isDeteleted());
                return ans;
            }

            public Version[] getAllVersions() {
                return versions.stream().toArray(Version[]::new);
            }

            private ConcurrentLinkedQueue<Version> versions = new ConcurrentLinkedQueue<>();

            /**
             * At least one suppliable version?
             */
            public boolean hasSuppliableVersion() {
                for (var v : versions) {
                    if (v.isSuppliable()) {
                        return true;
                    }
                }
                return false;
            }

            // return the last Version of the file available
            // to be returned to clients
            public Version getLastSuppliableVersion() {
                Version ans = null;
                for (var candidate : versions) {
                    // is version suppliable?
                    if (candidate.isSuppliable()) {
                        if (ans == null ||
                            ans.getLastUpdateTimestamp()
                                .compareTo(candidate.getLastUpdateTimestamp()) < 0 ||
                            // deleted version remplace available version
                            (ans.getLastUpdateTimestamp()
                                .compareTo(candidate.getLastUpdateTimestamp()) == 0
                                && candidate.isDeleted())) {
                            ans = candidate;
                        }
                    }
                }
                return ans;
            }

            public Version searchVersionByMetadata(FilenameMetadata metadata) {
                LocalFileInfo.Version ans = null;
                if (metadata != null && metadata.holdMetadata()) {
                    // there is a version corresponding to supplied metadata?
                    for (var version : this.versions) {
                        // find right version
                        if (version.matchMetadata(metadata)) {
                            ans = version;
                            break;
                        }
                    }
                } else {
                    // return last available version if not metadata found
                    ans = getLastSuppliableVersion();
                }
                return ans;
            }
        }

        // Concurrent map of local available files
        private ConcurrentSkipListMap<String, LocalFileInfo> localFiles = new ConcurrentSkipListMap<>();

        public LocalFileInfo.Version searchVersionByMetadata(String searchPath, FilenameMetadata metadata) {
            LocalFileInfo.Version ans = null;
            // search file by name
            var lfi = localFiles.get(searchPath);
            if (lfi == null) {
                // not found
                return null;
            }
            // check metadata
            ans = lfi.searchVersionByMetadata(metadata);
            return ans;
        }

        public LocalFileInfo.Version getLastSuppliableVersion(String searchPath) {
            return searchVersionByMetadata(searchPath, null);
        }

        /**
         * Check if downloading the remote file would
         */
        public boolean isRemoteVersionDownloadable(RemoteFileInfo remoteFile) {
            var last = getLastSuppliableVersion(remoteFile.getSearchPath().toString());
            if (last == null) {
                return true;
            }
            return last.isRemoteNewer(remoteFile);
        }

        /**
         * Map searchPath for a file (the one receiven in
         * HTTP requests) and map it into the local file version
         */
        public it.sssupserver.app.base.Path findTruePath(String path) {
            var lfi = localFiles.get(path);
            if (lfi == null) {
                return null;
            }
            var v = lfi.getLastSuppliableVersion();
            /* deletions are hidden from normal clients */
            if (v == null || v.isDeleted()) {
                return null;
            }
            return v.getPath();
        }

        /**
         * Check the last version of the a file here held.
         * If it exists compare with the to-be-deleted one:
         *  - if it is newer do nothing
         *  - if it is older (or equal) replace with this
         * @throws Exception
         */
        public void addDeletedVersion(RemoteFileInfo deleted) throws Exception {
            var searchPath = deleted.getSearchPath().toString();
            // find last version
            var last = getLastSuppliableVersion(searchPath);
            if (last == null) {
                // create new node and add version
                // TODO: get inspiration from DELETE /file
                // get ETag of the file
                var eTag = deleted.getETag();
                var dc = new StringBuilder()
                    .append(eTag)
                    .append('\n')
                    .toString();
                // generate content of new file
                var bytecontent = dc.getBytes(StandardCharsets.UTF_8);
                // put content in a buffer
                var w = BufferManager.getBuffer();
                var buf = w.get();
                buf.put(bytecontent);
                buf.flip();
                // metadata
                var metadata = new FilenameMetadata(
                    deleted.getSearchPath().getBasename(),
                    deleted.getBestVersion().getTimestamp(),
                    false,
                    false,
                    true);
                // file name
                var dstPath = deleted.getSearchPath().getDirname()
                    .createSubfile(metadata.toString());
                // schedule creation of file
                var cc = QueableCreateCommand.submit(executor, dstPath, identity, w);
                if (cc.getFuture().get() != true) {
                    throw new RuntimeException("Failed to delete file: " + searchPath);
                }
                // add new node
                var newNode = addRegularFileNode(dstPath);
                newNode.setSize(deleted.getBestVersion().getSize());
                newNode.setFileHash(deleted.getBestVersion().getHashAlgorithm(), deleted.getBestVersion().getFileHash());
            } else if (last.isRemoteNewer(deleted)) {
                last.markAsDeleted();
            }
            // otherwise do nothing
        }

        /**
         * Take a reference to a node and add the file to
         * @param node
         * @return
         * @throws Exception
         */
        public LocalFileInfo addLocalFileInfo(Node node) throws Exception {
            // assert node reference to file
            if (!node.isRegularFile()) {
                throw new IllegalArgumentException("Bad file (is not regular file): " + node.getPath().toString());
            }

            // TODO: extract file metadata from name
            var filePath = node.getPath();
            var basename = filePath.getBasename();
            var dirname = filePath.getDirname();
            // extract metadata
            var metadata = new FilenameMetadata(basename);
            // extract search path
            var searchPath = dirname.createSubfile(metadata.getSimpleName());
            // assert search path is ok:
            if (!isValidPathName(searchPath)) {
                throw new RuntimeException("Invalid search path ("
                    + searchPath + ") for file: " + node.getPath().toString());
            }
            // if the file should be renamed after this check
            boolean shouldMove = false;
            // if is corrupted do nothing
            if (metadata.isCorrupted()) {
                // do nothing
            } else if (metadata.isTemporary()) {
                // not @tmp should be found after restart, mark as corrupted
                metadata.setCorrupted(true);
                shouldMove = true;
            } else if (metadata.getTimestamp() == null) {
                // set timestamp
                metadata.setTimestamp(startInstant);
                shouldMove = true;
            }
            node.setLastModified(metadata.getTimestamp());
            // should rename the file?
            if (shouldMove) {
                // new file name
                var finalName = metadata.toString();
                var dstPath = dirname.createSubfile(finalName);
                var f = FutureMoveCommand.move(executor, filePath, dstPath, identity);
                // wait for move completion
                f.get();
                // new filePath
                filePath = dstPath;
                // update node
                node.rename(dstPath);
            }
            // Search for other file versions
            var lfi = localFiles.computeIfAbsent(searchPath.toString(), LocalFileInfo::new);
            // check if isDeleted() and not isCorrupted()
            var wasDeletedButNotCorrupted = metadata.isDeteleted() && !metadata.isCorrupted();
            // add new version
            var version = lfi.addVersion(node, metadata);
            if (wasDeletedButNotCorrupted && version.isCorrupted()) {
                // corrupted!
                metadata = version.getMetadata();
                // new file name
                var finalName = metadata.toString();
                var dstPath = dirname.createSubfile(finalName);
                var f = FutureMoveCommand.move(executor, filePath, dstPath, identity);
                // wait for move completion
                f.get();
                // new filePath
                filePath = dstPath;
                // update node
                node.rename(dstPath);
            }
            return lfi;
        }

        public void handleRegularFileInfo() throws Exception {
            for (var rf : snapshot.getRegularFileNodes()) {
                addLocalFileInfo(rf);
            }
        }

        /**
         * Return array of suppliable files
         */
        public LocalFileInfo[] getSuppliableFiles() {
            return localFiles.values().stream()
                .filter(LocalFileInfo::hasSuppliableVersion)
                .toArray(LocalFileInfo[]::new);
        }

        public String suppliableFilesAsJson(boolean prettyPrinting, boolean detailed) {
            var gBuilder = new GsonBuilder()
                .registerTypeAdapter(ManagedFileSystemStatus.LocalFileInfo.class, new LocalFileVersionGson(detailed));
            if (prettyPrinting) {
                gBuilder = gBuilder.setPrettyPrinting();
            }
            // add reference to serializer
            var gson = gBuilder.create();
            var files = getSuppliableFiles();
            var json = gson.toJson(files);
            return json;
        }

        public String suppliableFilesAsJson(boolean prettyPrinting) {
            return suppliableFilesAsJson(prettyPrinting, false);
        }

        public String suppliableFilesAsJson() {
            return suppliableFilesAsJson(false);
        }

        // return list of empty directories
        public List<Node> findEmptyDirectories() {
            return snapshot.filter(n -> n.isDirectory() && n.countChildren() == 0);
        }

        public Node[] getAllNodes() {
            return snapshot.getAllNodes();
        }

        public Node[] getAllRegularFileNodes() {
            return snapshot.getRegularFileNodes();
        }

        // test
        private void assertValidDirectoryNames() {
            var badFiles = Arrays.stream(snapshot.getDirectorysNodes())
                .filter(n -> !n.isRoot())
                .filter(n -> !isValidPathName(n.getPath()))
                .toArray(Node[]::new);
            if (badFiles.length > 0) {
                var sb = new StringBuilder("Bad file names:");
                for (var b : badFiles) {
                    sb.append(" '").append(b.getPath().toString()).append("';");
                }
                throw new RuntimeException(sb.toString());
            }
        }

        // calculate file hashes and sizes
        private void calculateFileHashes() throws InterruptedException, ExecutionException {
            // get only regular files
            var fileNodes = snapshot.getRegularFileNodes();
            // schedule file hash calculation
            var fh = Arrays.stream(fileNodes).map((Function<Node,Future<byte[]>>)n -> {
                var filename = n.getPath();
                try {
                    return FileReducerCommand.reduceByHash(executor, filename, identity, HASH_ALGORITHM);
                } catch (Exception e) {
                    e.printStackTrace();
                    var ff = new CompletableFuture<byte[]>();
                    ff.completeExceptionally(e);
                    return ff;
                }
            }).collect(Collectors.toList());
            // schedule file size calculation
            var fs = Arrays.stream(fileNodes).map((Function<Node,Future<Long>>)n -> {
                var filename = n.getPath();
                try {
                    return FutureFileSizeCommand.querySize(executor, filename, identity);
                } catch (Exception e) {
                    e.printStackTrace();
                    var ff = new CompletableFuture<Long>();
                    ff.completeExceptionally(e);
                    return ff;
                }
            }).collect(Collectors.toList());
            // attach hashes to files
            int index = 0;
            var fsIter = fs.iterator();
            for (Future<byte[]> future : fh) {
                // get hash
                var hash = future.get();
                fileNodes[index].setFileHash(HASH_ALGORITHM, hash);
                // get sizes
                var size = fsIter.next().get();
                fileNodes[index].setSize(size);
                // next
                ++index;
            }
            // TODO: mutable data structure olding file hashes
        }

        // check files owned by this datanode
        // and assert they names are all compliant
        // whit specified constaints
        public ManagedFileSystemStatus() throws Exception {
            // retrieve image of the file system
            var f = ListTreeCommand.explore(executor, "", identity);
            snapshot = f.get();
            // check all names are valid
            assertValidDirectoryNames();
            // calculate file hashes
            calculateFileHashes();
            // move (if necessary) and extract metadata from regular files
            // handle specially @deleted files
            handleRegularFileInfo();

            // put snapshot inside a mutable structure
            // in order to facilitate upload/deletion
            // operations

        }

        public String regularFilesInfoToJson(boolean prettyPrinting) {
            var gBuilder = new GsonBuilder()
                .registerTypeAdapter(Node.class, new NodeGson());
            if (prettyPrinting) {
                gBuilder = gBuilder.setPrettyPrinting();
            }
            // add reference to serializer
            var gson = gBuilder.create();

            // reference to regular files only
            var fileNodes = snapshot.getRegularFileNodes();
            var json = gson.toJson(fileNodes);
            return json;
        }

        public String regularFilesInfoToJson() {
            return regularFilesInfoToJson(false);
        }
    }


    private ManagedFileSystemStatus fsStatus;

    /**
     * Auxiliary class used to periodically check download requests
     * and schedule them
     */
    private DownloadManager downloadManager = new DownloadManager(this);
    private class DownloadManager implements Runnable {
        public static final long DOWNLOAD_PERIOD = 15;
        public static final TimeUnit PERIOD_UNIT = TimeUnit.SECONDS;

        private SimpleCDNHandler handler;

        public DownloadManager(SimpleCDNHandler handler) {
            this.handler = handler;
        }

        /**
         * Data structure used to coordinate requests of files to be
         * downloaded.
         */
        private final ConcurrentMap<String, RemoteFileInfo> downloadRequests = new ConcurrentSkipListMap<>();

        // used to prevent mixing
        private ConcurrentSkipListSet<String> locks = new ConcurrentSkipListSet<>();

        private ScheduledFuture<?> schedule;
        public void start() {
            schedule = this.handler.getTimedThreadPool()
            .scheduleWithFixedDelay(this, DOWNLOAD_PERIOD, DOWNLOAD_PERIOD, PERIOD_UNIT);
        }
        public void stop() {
            schedule.cancel(false);
            downloadRequests.clear();
        }

        private void startDownloadOrDeletion(String searchPath, RemoteFileInfo file) throws Exception {
            // check for deletion
            if (file.getBestVersion().isDeleted()) {
                // no need to delete a remote node
                fsStatus.addDeletedVersion(file);
                // release lock
                locks.remove(searchPath);
            } else {
                // schedule download
                handler.scheduleDownload(file, () -> locks.remove(searchPath));
            }
        }

        @Override
        public void run() {
            // iterate over all requests and schedule them
            for (var req : downloadRequests.entrySet()) {
                var searchPath = req.getKey();
                // try to "lock" key
                if (locks.add(searchPath)) {
                    // remove request
                    var file = downloadRequests.remove(searchPath);
                    // schedule download
                    try {
                        startDownloadOrDeletion(searchPath, file);
                    } catch (Exception e) {
                        // prevent livelock
                        locks.remove(searchPath);
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        }

        public void addDownloadRequest(RemoteFileInfo file) {
            var searchPath = file.getSearchPath().toString();
            downloadRequests.merge(searchPath, file, (v1, v2) -> {
                return v1.merge(v2);
            });
        }

        /**
         * Handle download of a single, specific file
         * Or deletion (that does not require download)
         */
    }

    /**
     * Callback used by PeerWatcher then receive the list of files
     * owned by the remote node
     */
    public void possibleFiles(DataNodeDescriptor peer, RemoteFileInfo[] files) {
        for (var file : files) {
            // should this node supply this file?
            var searchPath = file.getSearchPath().toString();
            if (topology.isFileSupplier(searchPath)) {
                // is file locally available?
                if (fsStatus.isRemoteVersionDownloadable(file)) {
                    // associate file to peer
                    file.addCandidateSupplier(peer);
                    // put file into possible download requests
                    // periodic task will handle it
                    downloadManager.addDownloadRequest(file);
                }
            }
        }
    }

    // HTTP server used to reply to clients
    private HttpServer clienthttpserver;
    private InetSocketAddress listening_client_address;

    // HTTP server used to receive management informations
    private HttpServer managerhttpserver;
    private InetSocketAddress listening_manager_address;

    // executor that will handle all locally-saved files!
    private FileManager executor;

    // handling CDN is complex, so it use
    public SimpleCDNHandler(FileManager executor, List<Map.Entry<String, String>> args) {
        // keep reference to file manager
        this.executor = executor;

        // parse all supplied arguments
        for (var e : args) {
            if (e.getKey().compareToIgnoreCase("config") == 0) {
                var path = e.getValue();
                var cwd = Paths.get("").toAbsolutePath();
                if (Files.isDirectory(Path.of(path))) {
                    config_file = cwd.resolve(Path.of(e.getValue(), DEFAULT_CONFIG_FILE));
                } else {
                    config_file = cwd.resolve(e.getValue());
                }
            }
        }

        // load configuration
        try {
            config = SimpleCDNConfiguration.parseJsonCondifuration(config_file.toString());
            System.out.println(config.toJson(true));
        } catch (FileNotFoundException e) {
            System.err.println("Error while parsing configuration file:");
            System.err.println(e);
            System.exit(1);
        }
        // associate identity with current instance
        identity = new Identity(config.getUser());
        // set thisnode parameters
        thisnode = new DataNodeDescriptor();
        thisnode.id = config.getnodeId();
        thisnode.dataendpoints = Arrays.stream(config.getClientEndpoints())
            .map(he -> he.getUrl()).toArray(URL[]::new);
        thisnode.managerendpoint = Arrays.stream(config.getManagementEndpoints())
            .map(he -> he.getUrl()).toArray(URL[]::new);
        thisnode.replication_factor = config.getReplicationFactor();
        // must also initialize topology
        topology = new Topology(this);
    }

    // rely on a thread pool in order to .
    private ExecutorService threadPool;
    // used to schedule commands after delay
    private ScheduledExecutorService timedThreadPool;

    public ExecutorService getThreadPool() {
        return threadPool;
    }

    public ScheduledExecutorService getTimedThreadPool() {
        return timedThreadPool;
    }

    @Override
    public void start() throws Exception {
        if (threadPool != null) {
            throw new RuntimeException("Handler already started");
        }
        threadPool = Executors.newCachedThreadPool();
        timedThreadPool = Executors.newScheduledThreadPool(1);
        httpClient = HttpClient.newBuilder()
            .version(java.net.http.HttpClient.Version.HTTP_1_1)
            .followRedirects(Redirect.ALWAYS)
            .connectTimeout(httpConnectionTimeout)
            .executor(threadPool)
            .build();

        // UTC start time
        startInstant = Instant.now();
        thisnode.initAllTimestamps(startInstant);
        thisnode.setStatus(DataNodeDescriptor.Status.RUNNING);

        // configuration is read inside constructor - so it is handled at startup!
        // discover owned files
        fsStatus = new ManagedFileSystemStatus();

        // start manager endpoint
        startManagementEndpoint();

        // start download manager
        downloadManager.start();

        // start join protocol:
        //  discover topology
        //  find previous node
        //  start replication strategy
        startPeerSearch();


        //  start client endpoint
        startClientEndpoints();

        // TODO Auto-generated method stub
        //throw new UnsupportedOperationException("Unimplemented method 'start'");
    }


    @Override
    public void stop() throws Exception {
        if (threadPool == null) {
            throw new RuntimeException("Handler not started.");
        }
        var stopInstant = Instant.now();
        thisnode.setStatus(DataNodeDescriptor.Status.STOPPING);
        thisnode.setLastStatusChange(stopInstant);

        // Give enough time to peers to discover status change
        Thread.sleep(30 * 1000);

        // start exit protocol
        // shutdown manager endpoint
        stopManagementEndpoint();

        // stop peer search
        stopPeerSearch();

        // stop download manager
        downloadManager.stop();

        // shutdown client endpoint
        // this is the last stage in order to reduce
        // service disruption to clients
        stopClientEndpoints();

        timedThreadPool.shutdown();
        threadPool.shutdown();
        timedThreadPool.awaitTermination(30, TimeUnit.SECONDS);
        threadPool.awaitTermination(30, TimeUnit.SECONDS);
        timedThreadPool = null;
        threadPool = null;

        thisnode.setStatus(DataNodeDescriptor.Status.SHUTDOWN);
        thisnode.setLastStatusChange(Instant.now());

        // TODO Auto-generated method stub
        //throw new UnsupportedOperationException("Unimplemented method 'stop'");
    }

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

    // this object is mantained alive for all the Handler lifespan
    private StatsCollector clientStatsCollector = new StatsCollector();

    private String regularFilesInfoWithMeToJson(boolean prettyPrinting) {
        var obj = new FileInfoWithIdentityGson(thisnode, fsStatus.getAllRegularFileNodes());
        var gBuilder = new GsonBuilder()
            .registerTypeAdapter(DataNodeDescriptor.class, new DataNodeDescriptorGson())
            .registerTypeAdapter(Node.class, new NodeGson())
            .registerTypeAdapter(FileInfoWithIdentityGson.class, obj);
        if (prettyPrinting) {
            gBuilder = gBuilder.setPrettyPrinting();
        }
        var gson = gBuilder.create();
        var json = gson.toJson(obj);
        return json;
    }

    private String topologyWithMeToJson(boolean prettyPrinting) {
        var obj = new TopologyWithIdentityGson(thisnode, topology.getSnapshot());
        var gBuilder = new GsonBuilder()
            .registerTypeAdapter(DataNodeDescriptor.class, new DataNodeDescriptorGson())
            .registerTypeAdapter(TopologyWithIdentityGson.class, obj);
        if (prettyPrinting) {
            gBuilder = gBuilder.setPrettyPrinting();
        }
        var gson = gBuilder.create();
        var json = gson.toJson(obj);
        return json;
    }

    private String suppliableFilesWithIdentity(boolean prettyPrinting, boolean detailed) {
        var obj = new SuppliableFilesWithIdentityGson(thisnode, fsStatus.getSuppliableFiles());
        var gBuilder = new GsonBuilder()
            .registerTypeAdapter(DataNodeDescriptor.class, new DataNodeDescriptorGson())
            .registerTypeAdapter(ManagedFileSystemStatus.LocalFileInfo.class, new LocalFileVersionGson(detailed))
            .registerTypeAdapter(SuppliableFilesWithIdentityGson.class, obj);
        if (prettyPrinting) {
            gBuilder = gBuilder.setPrettyPrinting();
        }
        var gson = gBuilder.create();
        var json = gson.toJson(obj);
        return json;

    }

    public String statisticsWithIdentity(boolean prettyPrinting) {
        var obj = new StatisticsWithIdentityGson(thisnode, clientStatsCollector.getFileStats());
        var gBuilder = new GsonBuilder()
            .registerTypeAdapter(DataNodeDescriptorGson.class, new DataNodeDescriptorGson())
            .registerTypeAdapter(StatsCollector.FileStats.class, new FileStatsGson())
            .registerTypeAdapter(StatsCollector.class, new StatsCollectorGson())
            .registerTypeAdapter(StatisticsWithIdentityGson.class, obj);
        if (prettyPrinting) {
            gBuilder = gBuilder.setPrettyPrinting();
        }
        var gson = gBuilder.create();
        var json = gson.toJson(obj);
        return json;
    }

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Operations associated with client endpoints ++++++++++++++++++++
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    class ClientHttpHandler implements HttpHandler {
        public static final String PATH = "/";

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                switch (exchange.getRequestMethod()) {
                    case "GET":
                        var requestedFile = new URI(PATH).relativize(exchange.getRequestURI()).getPath();
                        // check if current node is file supplier or redirect
                        if (SimpleCDNHandler.this.testSupplyabilityOrRedirect(requestedFile, exchange)) {
                            // prevent clients from directly accessing local files
                            var truePath = fsStatus.findTruePath(requestedFile);
                            if (truePath == null) {
                                // return 404
                                httpNotFound(exchange);
                            } else {
                                try {
                                    // recicle old working code
                                    HttpSchedulableReadCommand.handle(SimpleCDNHandler.this.executor, exchange, SimpleCDNHandler.this.identity, truePath);
                                } catch (Exception e) { System.err.println(e); e.printStackTrace(); }
                            }
                        }
                        break;
                    default:
                        httpMethodNotAllowed(exchange);
                        break;
                }
            } catch (Exception e) {
                httpInternalServerError(exchange);
                System.err.println(e);
                e.printStackTrace();
            }
        }
    }

    // flag for client request handling
    private boolean clientStarted = false;
    // list of addresses used to receive clients requests
    private List<InetSocketAddress> clientAddresses;
    // http handlers associated with clients
    private List<HttpServer> clientHttpServers;

    private void startClientEndpoints() throws IOException {
        if (clientStarted) {
            throw new RuntimeException("Cannot start twice, listener already working");
        }
        if (clientAddresses == null) {
            // endpoints are specified inside configuration,
            // they do not change after restart
            var ces = getClientEndpoints();
            if (ces.length == 0) {
                throw new RuntimeException("Missing endpoints for client!");
            }
            // preallocate lists
            clientAddresses = new ArrayList<>(ces.length);
            clientHttpServers = new ArrayList<>(ces.length);
            // otherwise bind all them
            for (var ce : ces) {
                var port = ce.getPort();
                var address = ce.getHost();
                // should be http or nothing
                if (ce.getProtocol() != null && !ce.getProtocol().equals("http")) {
                    throw new RuntimeException("Bad url (unsupported protocol): " + ce);
                }
                clientAddresses.add(new InetSocketAddress(address, port));
            }
        }
        // activate all endpoints
        for (var addr : clientAddresses) {
            var hs = HttpServer.create(addr, 30);
            // Add http handler for clients
            hs.createContext("/", new ClientHttpHandler());
            clientHttpServers.add(hs);
            hs.setExecutor(threadPool);
            hs.start();
        }

        clientStarted = true;
    }

    private void stopClientEndpoints() {
        if (!clientStarted) {
            throw new RuntimeException("No listener working");
        }

        // disable all client http servers
        for (var chs : clientHttpServers) {
            chs.stop(5);
        }
        clientHttpServers.clear();
        // maintain space for possible new restarts
        ((ArrayList<HttpServer>)clientHttpServers).ensureCapacity(clientAddresses.size());

        clientStarted = false;
    }
    // ----------------------------------------------------------------
    // Operations associated with client endpoints --------------------
    // ----------------------------------------------------------------

    private static void sendJson(HttpExchange exchange, String json) throws IOException {
        var bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        var os = exchange.getResponseBody();
        os.write(bytes);
        exchange.close();
    }

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Operations associated with management endpoints ++++++++++++++++
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // listen for /api calls
    private class ApiManagementHttpHandler implements HttpHandler {
        public static final String PATH = "/api";

        private void sendTopology(HttpExchange exchange) throws IOException {
            var topo = topology.asJsonArray();
            sendJson(exchange, topo);
        }

        private void sendTopologyWithIdentity(HttpExchange exchange) throws IOException {
            var topo = topologyWithMeToJson(false);
            sendJson(exchange, topo);
        }

        private void sendIdentity(HttpExchange exchange) throws IOException {
            var me = thisnodeAsJson();
            sendJson(exchange, me);
        }

        private void sendLocalStorageInfo(HttpExchange exchange) throws IOException {
            var me = fsStatus.regularFilesInfoToJson();
            sendJson(exchange, me);
        }

        private void sendLocalStorageInfoWithIdentity(HttpExchange exchange) throws IOException {
            var info = regularFilesInfoWithMeToJson(false);
            sendJson(exchange, info);
        }

        private void sendSuppliableFileInfo(HttpExchange exchange) throws IOException {
            var info = fsStatus.suppliableFilesAsJson();
            sendJson(exchange, info);
        }

        private void sendSuppliableFileInfoWithIdentity(HttpExchange exchange) throws IOException {
            var info = suppliableFilesWithIdentity(false, false);
            sendJson(exchange, info);
        }

        private void sendSuppliableFileInfoDetailed(HttpExchange exchange) throws IOException {
            var info = fsStatus.suppliableFilesAsJson(true, true);
            sendJson(exchange, info);
        }

        private void sendSuppliableFileInfoDetailedWithIdentity(HttpExchange exchange) throws IOException {
            var info = suppliableFilesWithIdentity(false, true);
            sendJson(exchange, info);
        }

        private void sendFileStatistics(HttpExchange exchange) throws IOException {
            var me = clientStatsCollector.asJson();
            sendJson(exchange, me);
        }

        private void sendFileStatisticsWithIdentity(HttpExchange exchange) throws IOException {
            var stats = statisticsWithIdentity(false);
            sendJson(exchange, stats);
        }

        private void sendConfiguration(HttpExchange exchange) throws IOException {
            var conf = config.toJson();
            sendJson(exchange, conf);
        }

        private void sendFileHash(HttpExchange exchange, String searchPath) throws IOException {
            if (!isValidPathName(searchPath)) {
                httpBadRequest(exchange, "Invalid path: " + searchPath);
            } else {
                var h = SearchPathWithHashGson.toJson(searchPath, topology.getFileHash(searchPath));
                sendJson(exchange, h);
            }
        }

        private void handlePOSThello(HttpExchange exchange) throws IOException {
            try {
                var is = exchange.getRequestBody();
                var json = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                var other = DataNodeDescriptor
                    .fromJson(json);
                // reply with me
                sendIdentity(exchange);
                // new node discovered?
                watchIfNotWatched(other);
            } catch (Exception e) {
                // TODO: handle exception
                httpBadRequest(exchange);
            }
        }

        private void handlePOSTtopology(HttpExchange exchange) throws IOException {
            try {
                // parse remote node info
                // they must be checked
                var is = exchange.getRequestBody();
                var json = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                var other = DataNodeDescriptor
                    .fromJson(json);
                // reply with seen topology info
                sendTopologyWithIdentity(exchange);
                // new node discovered?
                watchIfNotWatched(other);
            } catch (Exception e) {
                // TODO: handle exception
                httpBadRequest(exchange);
            }
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                var path = new URI(PATH).relativize(exchange.getRequestURI()).getPath();
                switch (exchange.getRequestMethod()) {
                    case "GET":
                        // retrieve node or topology information
                        {
                            // TODO - check if it contains path
                            if (path.equals("topology")) {
                                // return topology
                                sendTopologyWithIdentity(exchange);
                            }
                            else if (path.equals("identity")) {
                                // return topology
                                sendIdentity(exchange);
                            }
                            else if (path.equals("storage")) {
                                // return topology
                                sendLocalStorageInfoWithIdentity(exchange);
                            }
                            else if (path.equals("suppliables")) {
                                // return topology
                                sendSuppliableFileInfoWithIdentity(exchange);
                            }
                            else if (path.equals("detailedSuppliables")) {
                                // return topology
                                sendSuppliableFileInfoDetailedWithIdentity(exchange);
                            }
                            else if (path.equals("stats")) {
                                // return file statistics
                                sendFileStatisticsWithIdentity(exchange);
                            }
                            else if (path.equals("configuration")) {
                                // return file statistics
                                sendConfiguration(exchange);
                            }
                            else if (path.startsWith("hash/")) {
                                sendFileHash(exchange, path.substring("hash/".length()));
                            }
                            else {
                                httpNotFound(exchange);
                            }
                        }
                        break;
                    case "POST":
                        // for join operations and so...
                        {
                            switch (path) {
                                case "hello":
                                    {
                                        // new node found
                                        handlePOSThello(exchange);
                                    }
                                    break;
                                case "topology":
                                    {
                                        handlePOSTtopology(exchange);
                                    }
                                    break;
                                default:
                                    httpNotFound(exchange);
                                    break;
                            }
                        }
                        break;
                    default:
                        httpMethodNotAllowed(exchange);
                }
            } catch (Exception e) {
                System.err.println(e);
                e.printStackTrace();
            }
        }

    }

    public HttpRequest buildApiTopologyPOST(URI uri) {
        var rUri = uri
            .resolve(ApiManagementHttpHandler.PATH + "/")
            .resolve("topology");
        var req = HttpRequest.newBuilder(rUri)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers
                .ofString(thisnodeAsJson(), StandardCharsets.UTF_8)
            )
            .build();
        return req;
    }

    public HttpRequest buildApiHelloPOST(URI uri) {
        var rUri = uri
            .resolve(ApiManagementHttpHandler.PATH + "/")
            .resolve("hello");
        var req = HttpRequest.newBuilder(rUri)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers
                .ofString(thisnodeAsJson(), StandardCharsets.UTF_8)
            )
            .build();
        return req;
    }

    public HttpRequest buildApiSuppliablesGET(URI uri) {
        var rUri = uri
            .resolve(ApiManagementHttpHandler.PATH + "/")
            .resolve("suppliables");
        var req = HttpRequest.newBuilder(rUri)
            .GET()
            .build();
        return req;
    }

    public HttpRequest buildFileDownloadGET(URI uri, RemoteFileInfo remote) {
        var searchPath = remote.getEffectivePath().toString();
        var rUri = uri
            .resolve(FileManagementHttpHandler.PATH + "/")
            .resolve(searchPath);
        var req = HttpRequest.newBuilder(rUri)
            .GET()
            .build();
        return req;
    }

    // listen for PUT and DELETE commands
    private class FileManagementHttpHandler implements HttpHandler {
        public static final String PATH = "/file";

        private void handleGET(HttpExchange exchange) throws Exception {
            // path to requsted file
            var requestedFile = new URI(PATH).relativize(exchange.getRequestURI()).getPath();
            // check if specific version of the file was required
            // extract metadata
            it.sssupserver.app.base.Path receivedPath = null;
            FilenameMetadata metadata = null;
            try {
                // try to parse received path
                receivedPath = new it.sssupserver.app.base.Path(requestedFile);
            } catch (Exception e) {
                httpBadRequest(exchange, "Bad path: " + requestedFile);
                return;
            }
            // does path contains metadata?
            metadata = new FilenameMetadata(receivedPath.getBasename());
            // get dirname
            var dirname = receivedPath.getDirname();
            // extract searchPath
            var searchPath = dirname.createSubfile(metadata.getSimpleName());
            if (!testSupplyabilityOrRedirectToManagement(searchPath.toString(), exchange)) {
                // file not available here
                return;
            }
            // search file version by metadata
            var candidateVersion = fsStatus.searchVersionByMetadata(searchPath.toString(), metadata);
            if (candidateVersion == null) {
                // 404 NOT FOUND
                httpNotFound(exchange, "File not found: " + requestedFile);
                return;
            } else if (candidateVersion.isDeleted()) {
                httpGone(exchange);
                return;
            }
            // check ETah HTTP header
            // ETag parameter for the local file version
            var localETag = candidateVersion.generateHttpETagHeader();
            if (exchange.getRequestHeaders().containsKey("ETag"))
            {
                // ETag parameter found in request
                var reqETag = exchange.getRequestHeaders().getFirst("ETag");
                // if they are equals do not send response back
                if (localETag.equals(reqETag)) {
                    // content not modified
                    httpNotModified(exchange);
                    return;
                }
            }
            // prepare to send back response
            var resH = exchange.getResponseHeaders();
            resH.set("ETag", localETag);
            // path of the file effectively returned
            var truePath = candidateVersion.getPath();
            // send file back
            HttpSchedulableReadCommand.handle(executor, exchange, identity, truePath);
            // DONE
        }

        private void handlePUT(HttpExchange exchange) throws Exception {
            // handle upload of file - a file is received by an admin application
            long receivedFileSize = 0;
            var md = MessageDigest.getInstance(ManagedFileSystemStatus.HASH_ALGORITHM);
            // extract path to file
            var requestedFile = new URI(PATH).relativize(exchange.getRequestURI()).getPath().toString();
            it.sssupserver.app.base.Path receivedPath = null;
            FilenameMetadata metadata = null;
            // check if path is ok - no '@' inside, isValidPathName
            {
                boolean badPath = false;
                try {
                    receivedPath = new it.sssupserver.app.base.Path(requestedFile);
                    // extract medatada
                    metadata = new FilenameMetadata(receivedPath.getBasename());
                    if (metadata.holdMetadata() || requestedFile.contains("@")) {
                        badPath = true;
                    }
                } catch (Exception e) {
                    badPath = true;
                }
                if (badPath) {
                    httpBadRequest(exchange, "Bad path: " + requestedFile);
                    return;
                }
            }
            var dirname = receivedPath.getDirname();
            // is this node owner of the file? Otherwise redirect
            if (!testOwnershipOrRedirectToManagement(PATH, exchange)) {
                return;
            }
            // get timestamp used to track file
            metadata.setTimestamp(Instant.now());
            // generate temporary download file name - "@tmp"
            metadata.setTemporary(true);
            var temporaryFilename = dirname.createSubfile(metadata.toString());
            // assert directory path creation
            FutureMkdirCommand.create(executor, dirname, identity).get();
            {
                long contentLength;
                var contentLengthHeader = exchange.getRequestHeaders().getFirst("Content-Length");
                if (contentLengthHeader != null) {
                    contentLength = Long.parseLong(contentLengthHeader);
                } else {
                    // just for safety
                    contentLength = Long.MAX_VALUE;
                }
                var is = exchange.getRequestBody();
                var bfsz = BufferManager.getBufferSize();
                var tmp = new byte[bfsz];
                BufferWrapper bufWrapper;
                int len;
                java.nio.ByteBuffer buf;
                // extract expected file size

                bufWrapper = BufferManager.getBuffer();
                buf = bufWrapper.get();
                // read until data availables or buffer filled
                while (contentLength > 0 && buf.hasRemaining()) {
                    len = is.read(tmp, 0, (int)Math.min((long)tmp.length, contentLength));
                    if (len == -1) {
                        break;
                    }
                    contentLength -= len;
                    receivedFileSize += len;
                    md.update(tmp, 0, len);
                    buf.put(tmp, 0, len);
                }
                buf.flip();
                // create file locally and start storing it
                QueableCommand queable = QueableCreateCommand.submit(executor, temporaryFilename, identity, bufWrapper);
                // store file piece by piece
                while (contentLength > 0) {
                    // take a new buffer
                    bufWrapper = BufferManager.getBuffer();
                    buf = bufWrapper.get();
                    // fill this buffer
                    while (contentLength > 0 && buf.hasRemaining()) {
                        len = is.read(tmp, 0, (int)Math.min((long)tmp.length, contentLength));
                        if (len == -1) {
                            break;
                        }
                        contentLength -= len;
                        receivedFileSize += len;
                        md.update(tmp, 0, len);
                        buf.put(tmp, 0, len);
                    }
                    buf.flip();
                    // append new
                    queable.enqueue(bufWrapper);
                }
                // wait for completion
                var success = queable.getFuture().get();
                if (!success) {
                    // delete temporary file
                    FutureDeleteCommand.delete(executor, temporaryFilename, identity).get();
                    throw new RuntimeException("Error while creating file: " + temporaryFilename);
                }
            }
            // rename file with final name - i.e. remove "@tmp"
            metadata.setTemporary(false);
            var finalName = dirname.createSubfile(metadata.toString());
            FutureMoveCommand.move(executor, temporaryFilename, finalName, identity).get();
            // add save new reference to file as available
            var newNode = fsStatus.addRegularFileNode(finalName);
            // Compute size
            newNode.setSize(receivedFileSize);
            newNode.setFileHash(md.getAlgorithm(), md.digest());
            var lfi = fsStatus.addLocalFileInfo(newNode);
            // TODO: delete possible old versions
            // send 201 CREATED
            httpCreated(exchange, "File saved as: " + finalName);
            // Local files updated!
            updateLastFileUpdateTimestamp();
        }

        private void handleDELETE(HttpExchange exchange) throws Exception {
            // should always target latest available version
            var searchPath = new URI(PATH).relativize(exchange.getRequestURI()).getPath();
            // can only delete last available version, then
            // requestedFile cannot include any "@"
            if (searchPath.contains("@")) {
                httpBadRequest(exchange, "Bad path: " + searchPath);
                return;
            }
            // only owner can accept delete
            if (!testOwnershipOrRedirectToManagement(searchPath, exchange)) {
                // file not available here
                return;
            }
            // file available here?
            var candidateVersion = fsStatus.getLastSuppliableVersion(searchPath);
            if (candidateVersion == null) {
                // 404 NOT FOUND
                httpNotFound(exchange, "File not found: " + searchPath);
                return;
            } else if (candidateVersion.isDeleted()) {
                httpGone(exchange);
                return;
            }
            // require file deletion
            candidateVersion.markAsDeleted();
            // confirm deletion
            httpOk(exchange, "Deleted file: " + searchPath);
            // Local files updated!
            updateLastFileUpdateTimestamp();
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                switch (exchange.getRequestMethod()) {
                    case "GET":
                        // download file - same as for users but include
                        // metadata inside HTTP response headers
                        handleGET(exchange);
                        break;
                    case "DELETE":
                        // delete file - handle deletion protocol
                        handleDELETE(exchange);
                        break;
                    case "PUT":
                        // upload - handle replication protocol
                        handlePUT(exchange);
                        break;
                    default:
                        httpMethodNotAllowed(exchange);
                }
            } catch (Exception e) {
                System.err.println(e);
                e.printStackTrace();
                // handle unexpected error
                httpInternalServerError(exchange);
            }
        }

    }

    private boolean managementStarted = false;
    // list of addresses used to receive (other) DataNodes requests
    private List<InetSocketAddress> managementAddresses;
    // http handlers associated with other nodes
    private List<HttpServer> managementHttpServers;

    private void startManagementEndpoint() throws IOException {
        if (managementStarted) {
            throw new RuntimeException("Cannot start twice, listener already working");
        }
        if (managementAddresses == null) {
            var mes = getManagementEndpoints();
            if (mes.length == 0) {
                throw new RuntimeException("Missing endpoints for DataNodes!");
            }
            managementAddresses = new ArrayList<>(mes.length);
            managementHttpServers = new ArrayList<>(mes.length);
            for (var me : mes) {
                var port = me.getPort();
                var address = me.getHost();
                // should be http or nothing
                if (me.getProtocol() != null && !me.getProtocol().equals("http")) {
                    throw new RuntimeException("Bad url (unsupported protocol): " + me);
                }
                managementAddresses.add(new InetSocketAddress(address, port));
            }
        }
        // add all endpoint
        for (var addr : managementAddresses) {
            var hs = HttpServer.create(addr, 30);
            // handle /api calls
            hs.createContext(ApiManagementHttpHandler.PATH, new ApiManagementHttpHandler());
            // handle GET, PUT and DELETE for admins only
            hs.createContext(FileManagementHttpHandler.PATH, new FileManagementHttpHandler());
            // set thread pool executor for parallelism
            hs.setExecutor(threadPool);
            managementHttpServers.add(hs);
            hs.start();
        }
        managementStarted = true;
    }

    private void stopManagementEndpoint() {
        if (!managementStarted) {
            throw new RuntimeException("No listener working");
        }
        // disable all management http handlers
        for (var chs : managementHttpServers) {
            chs.stop(5);
        }
        managementHttpServers.clear();
        // maintain space for possible new restarts
        ((ArrayList<HttpServer>)managementHttpServers).ensureCapacity(managementAddresses.size());

        managementStarted = false;
    }

    // ----------------------------------------------------------------
    // Operations associated with management endpoints ----------------
    // ----------------------------------------------------------------


    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Operations associated with control thread ++++++++++++++++++++++
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Taking care of a distributed system is a complex task, many
    // things could fail in many different ways. Fails might also
    // occur silently without notice during normal interactions among
    // nodes.
    // A special thread (pool) will be in charge of trying to detect
    // failure of other nodes, changes in the topology and replication
    // of locally owned files.

    /**
     * List of peers to be monitored.
     * Associate URL with PeerWatcher
     */
    private ConcurrentSkipListMap<String, PeerWatcher> candidates;
    /**
     * Used to retrieve watchers once they have been associated with
     * a remote peer
     */
    private ConcurrentSkipListMap<Long, PeerWatcher> candidatesById;

    private void watchIfNotWatched(DataNodeDescriptor remoteNode) throws URISyntaxException {
        var rn = topology.findDataNodeDescriptorById(remoteNode.getId());
        if (rn == null && isDataNodeCompatible(remoteNode)) {
            for (var me : remoteNode.getManagerendpoint()) {
                watch(me);
            }
        }
    }

    /**
     * Start watching for a remote peer
     * @param url
     * @throws URISyntaxException
     */
    private void watch(URL url) throws URISyntaxException {
        var pw = new PeerWatcher(this, url);
        // register new watcher - only if necessary
        candidates.computeIfAbsent(url.toString(), s -> {
            pw.start();
            return pw;
        });
    }

    // start control thread, return when JOIN protocol is terminated
    private void startPeerSearch() throws URISyntaxException {
        candidates = new ConcurrentSkipListMap<>();
        candidatesById = new ConcurrentSkipListMap<>();
        var cep = getConfig().getCandidatePeers();
        for (var ce : cep) {
            watch(ce.getUrl());
        }
    }

    /**
     * Callback used by PeerWatcher(s) when they discover a remote
     * peer.
     * Return true if association success, otherwise false.
     * If true, Watcher should interrupt keepalive
     */
    public boolean associatePeerWithWatcher(DataNodeDescriptor peer, PeerWatcher watcher) {
        // is there already a watcher for this peer?
        var ans = candidatesById.compute(peer.getId(),
            (BiFunction<Long,PeerWatcher,PeerWatcher>)
            (k, v) -> {
                // assert idempotence, used when remote change
                if (v == watcher) {
                    return v;
                }
                // was a watcher already associated with
                // the remote peer?
                if (v != null) {
                    // same instance?
                    if (peer.describeSameInstance(v.getAssociatedPeer())) {
                        // is this case do nothing
                        return v;
                    } else {
                        // disable previous
                        v.unbind();
                        v.stop();
                        // remove watcher from URL-PeerWatcher map
                        candidates.compute(v.getUrl().toString(), (kk, vv) -> {
                            if (v == vv) {
                                return null;
                            }
                            // else do nothing
                            return vv;
                        });
                    }
                }
                // associate node with
                watcher.bind(peer);
                // add node to topology
                topology.addDataNodeDescriptor(peer);
                return watcher;
            }
        );
        // was this watcher associated with the peer?
        var associated = ans == watcher;
        if (!associated) {
            // stop peer
            watcher.start();
        }
        return associated;
    }

    /**
     * Called by a PeerWatcher when it consider a remote node
     * failed or dead
     * @param peers
     * @throws URISyntaxException
     */
    public void decouplePeerFromWatcher(DataNodeDescriptor peer, PeerWatcher watcher) {
        var w = candidatesById.compute(peer.getId(), (k, v) -> {
            if (watcher == v) {
                return null;
            } else {
                return v;
            }
        });
        if (w == watcher) {
            watcher.unbind();
        }
        // after 120 seconds, schedule node removal from topology
        getTimedThreadPool()
        .schedule(() -> {
            topology.removeDataNodeDescriptor(peer);
        }, 120, TimeUnit.SECONDS);
    }

    /**
     * Callback used by PeerWatcher(s) when they receive topology
     * updates.
     * @throws URISyntaxException
     */
    public void possiblePeers(DataNodeDescriptor[] peers) throws URISyntaxException {
        for (var peer : peers) {
            // check only compatible peers
            if (this.isDataNodeCompatible(peer)) {
                // is this peer already known?
                var known = topology.findDataNodeDescriptorById(peer.getId());
                if (known == null) {
                    // new to watch
                    for (var url : peer.getManagerendpoint()) {
                        watch(url);
                    }
                } else {
                    // if equals do nothing by default
                    // Possible update: fast propagation
                    // of topology updates in the future
                }
            }
        }
    }

    private void stopPeerSearch() {
        // stop all pairs
        for (var pw : candidates.values()) {
            pw.stop();
        }
        candidates = null;
        candidatesById = null;
    }

    /**
     * Schedule sent of notification for new file upload
     * Should be called on startup and after /file PUT
     * @param lfi
     */
    public void scheduleNotificationForNewFileUpload(ManagedFileSystemStatus.LocalFileInfo lfi) {
        // TODO: notify neighbour nodes to ask them starting
        // replication strategy
        // TODO: request operation to thread pool
    }

    // ----------------------------------------------------------------
    // Operations associated with control thread ----------------------
    // ----------------------------------------------------------------

    // schedule read command to obtain file content and
    // send it to remote node
    private Future sendFileToDataNode(URI remoteDataNode, Path file) {

        return null;
    }

    public URL getRemoteManagementEndpointByNodeId(long nodeId) {
        // get reference to remote node
        var remoteNode = topology.searchDataNodeDescriptorById(nodeId);
        if (remoteNode == null) {
            throw new RuntimeException("No node with id " + nodeId + " found");
        }
        // get management endpoint
        var mep = remoteNode.getRandomManagementEndpointURL();
        return mep;
    }

    /**
     * send a GET /file/path/to/file request to a remote node to download it
     * and store locally
     * @throws URISyntaxException
     * @throws InterruptedException
     * @throws IOException
     */
    private boolean downloadFileFromDatanode(String searchPath, long nodeId) throws URISyntaxException, IOException, InterruptedException {
        var filePath = new it.sssupserver.app.base.Path(searchPath);
        var dirname = filePath.getDirname();
        if (nodeId == thisnode.id) {
            throw new RuntimeException("Cannot download from itself");
        }
        // get management endpoint
        var me = getRemoteManagementEndpointByNodeId(nodeId);
        // build request URI
        var rUri = me.toURI().resolve(FileManagementHttpHandler.PATH + "/").resolve(searchPath);
        // check if local version exists
        var version = fsStatus.getLastSuppliableVersion(searchPath);
        // Build request
        var reqB = HttpRequest.newBuilder()
            .GET()
            .uri(rUri);
        if (version != null) {
            reqB = reqB.header("ETag", version.generateHttpETagHeader());
        }
        var req = reqB.build();
        var res = httpClient.send(req, BodyHandlers.ofInputStream());
        var is = res.body();
        switch (res.statusCode()) {
            case 200:
                // store file locally
                {
                    ETagParser receivedETag;
                    try {
                        // check received ETag
                        var receivedETagHeader = res.headers().firstValue("ETag").get();
                        receivedETag = new ETagParser(receivedETagHeader);
                        var metadata = new FilenameMetadata(filePath.getBasename(),
                            receivedETag.getTimestamp(), false, true, false);
                        // check coerence with file dimension
                        var contentLength = Long.parseLong(res.headers().firstValue("Content-Length").get());
                        if (contentLength != receivedETag.getSize()) {
                            throw new RuntimeException("ETag file size is " + receivedETag.getSize()
                                + " but Content-Lenght is " + contentLength);
                        }
                        // is file hash known?
                        var ha = receivedETag.getHashAlgorithm();
                        if (!Arrays.stream(FileReducerCommand.getAvailableHashAlgorithms())
                            .anyMatch(ha::equals)) {
                            throw new RuntimeException("Bad ETag (unknown hash): " + receivedETag);
                        }
                        // create hasher for content check
                        var md = MessageDigest.getInstance(ha);
                        // path to save temporary file during download
                        var temporaryFilename = dirname.createSubfile(metadata.toString());
                        // create directory
                        FutureMkdirCommand.create(executor, dirname, identity).get();
                        // download file piece by piece
                        {
                            var bfsz = BufferManager.getBufferSize();
                            var tmp = new byte[bfsz];
                            BufferWrapper bufWrapper;
                            int len;
                            java.nio.ByteBuffer buf;
                            long receivedFileSize = 0;

                            bufWrapper = BufferManager.getBuffer();
                            buf = bufWrapper.get();
                            // read until data availables or buffer filled
                            while (contentLength > 0 && buf.hasRemaining()) {
                                len = is.read(tmp, 0, (int)Math.min((long)tmp.length, contentLength));
                                if (len == -1) {
                                    break;
                                }
                                contentLength -= len;
                                receivedFileSize += len;
                                md.update(tmp, 0, len);
                                buf.put(tmp, 0, len);
                            }
                            buf.flip();
                            // create file locally and start storing it
                            QueableCommand queable = QueableCreateCommand.submit(executor, temporaryFilename, identity, bufWrapper);
                            // store file piece by piece
                            while (contentLength > 0) {
                                // take a new buffer
                                bufWrapper = BufferManager.getBuffer();
                                buf = bufWrapper.get();
                                // fill this buffer
                                while (contentLength > 0 && buf.hasRemaining()) {
                                    len = is.read(tmp, 0, (int)Math.min((long)tmp.length, contentLength));
                                    if (len == -1) {
                                        break;
                                    }
                                    contentLength -= len;
                                    receivedFileSize += len;
                                    md.update(tmp, 0, len);
                                    buf.put(tmp, 0, len);
                                }
                                buf.flip();
                                // append new
                                queable.enqueue(bufWrapper);
                            }
                            // wait for completion
                            var success = queable.getFuture().get();
                            if (!success) {
                                // delete temporary file
                                FutureDeleteCommand.delete(executor, temporaryFilename, identity);
                                throw new RuntimeException("Error while creating file: " + temporaryFilename);
                            }
                            // rename file with final name - i.e. remove "@tmp"
                            metadata.setTemporary(false);
                            var finalName = dirname.createSubfile(metadata.toString());
                            FutureMoveCommand.move(executor, temporaryFilename, finalName, identity);
                            // add save new reference to file as available
                            var newNode = fsStatus.addRegularFileNode(finalName);
                            // Compute size
                            newNode.setSize(receivedFileSize);
                            newNode.setFileHash(md.getAlgorithm(), md.digest());
                            var lfi = fsStatus.addLocalFileInfo(newNode);
                            is.close();
                            return true;
                        }
                    } catch (Exception e) {
                        is.close();
                    }
                }
                break;
            case 410:
                // handle deleted file
                {
                    ;
                }
                break;
            default:
                break;
        }
        return false;
    }

    private void downloadLocalStorageInfoWithIdentityFromRemoteDatanode(long nodeId) throws URISyntaxException, IOException, InterruptedException {
        var me = getRemoteManagementEndpointByNodeId(nodeId);
        // request uri
        var rUri = me.toURI().resolve(ApiManagementHttpHandler.PATH + "/").resolve("suppliables");
        // build http request
        var req = HttpRequest.newBuilder(rUri)
            .GET()
            .timeout(httpRequestTimeout)
            .build();
        var res = httpClient.send(req, BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (res.statusCode() != 200) {
            // bad result
            return;
        }
    }

    public static String generateHttpETagHeader(
        Instant lastModified,
        long size,
        String hashAlgorithm,
        byte[] fileHash
    ) {
        return lastModified.toEpochMilli()
            + ":" + size
            + ":" + hashAlgorithm
            + ":" + bytesToHex(fileHash);
    }

    public void scheduleDownload(RemoteFileInfo file, Runnable callback) {
        var downloader = new FileDownloader(
            file,
            callback
        );
        scheduleDownload(downloader);
    }

    protected void scheduleDownload(FileDownloader downloader) {
        // schedule download
        getThreadPool().submit(downloader);
    }

    /**
     * Auxiliary class used to schedule file downloads
     */
    public class FileDownloader implements Runnable, BiConsumer<HttpResponse<Void>,Throwable>,
        Flow.Subscriber<List<ByteBuffer>>, HttpResponse.BodyHandler<Void> {

        private int errorCounter = 0;
        private Instant lastError;

        private it.sssupserver.app.base.Path downloadPath;
        private it.sssupserver.app.base.Path finalPath;
        private it.sssupserver.app.base.Path dirname;

        /**
         * Rename file after download
         */
        private void renameFileAfterDownload() throws InterruptedException, ExecutionException, Exception {
            FutureMoveCommand.move(executor, downloadPath, finalPath, identity).get();
        }

        /**
         * In case of error during download, remove bad file
         */
        private void deleteBadDownload() throws InterruptedException, ExecutionException, Exception {
            FutureDeleteCommand.delete(executor, downloadPath, identity).get();
        }

        private RemoteFileInfo fileToDownload;
        private Runnable onTermination;
        private List<DataNodeDescriptor> suppliers;
        public FileDownloader(RemoteFileInfo fileToDownload, Runnable onTermination) {
            this.fileToDownload = fileToDownload;
            this.onTermination = onTermination;
            this.suppliers = fileToDownload.getCandidateSuppliers();
            // path used for download
            downloadPath = fileToDownload.getDownloadPath();
            finalPath = fileToDownload.getEffectivePath();
            dirname = fileToDownload.getSearchPath().getDirname();
        }

        private void onError() {
            ++errorCounter;
            lastError = Instant.now();
            // New download attempt
            if (!suppliers.isEmpty()) {
                // reschedule
                SimpleCDNHandler.this.getThreadPool().submit(this);
            } else {
                // release lock
                onTermination.run();
                failed = false;
            }
        }

        /**
         * Used from Flow.Subscriber
         */
        private boolean failed = false;
        @Override
        public void accept(HttpResponse<Void> res, Throwable err) {
            if (failed || err != null || (res != null && res.statusCode() != 200)) {
                onError();
            } else {
                // register new node
                var newNode = fsStatus.addRegularFileNode(finalPath);
                newNode.setSize(fileToDownload.getBestVersion().getSize());
                newNode.setFileHash(fileToDownload.getBestVersion().getHashAlgorithm(), fileToDownload.getBestVersion().getFileHash());
                // ok, release lock
                onTermination.run();
            }
        }

        /**
         * Send http request
         */
        @Override
        public void run() {
            try {
                // peer candidate supplier
                var supplier = suppliers.remove(0);
                // remote endpoint
                var rem = supplier.getRandomManagementEndpointURL().toURI();
                // build download request
                var req = SimpleCDNHandler.this.buildFileDownloadGET(rem, fileToDownload);
                SimpleCDNHandler.this.getHttpClient()
                    .sendAsync(req, this)
                    .whenCompleteAsync(this, SimpleCDNHandler.this.getThreadPool());
            } catch (URISyntaxException e) {
                e.printStackTrace();
                // maybe reschedule
                onError();
            }
        }

        // How to handle bad response
        //  https://stackoverflow.com/questions/56025114/how-do-i-get-the-status-code-for-a-response-i-subscribe-to-using-the-jdks-httpc
        // Sample on doc:
        //  HttpRequest request = HttpRequest.newBuilder()
        //    .uri(URI.create("http://www.foo.com/"))
        //    .build();
        //  BodyHandler<Path> bodyHandler = (rspInfo) -> rspInfo.statusCode() == 200
        //    ? BodySubscribers.ofFile(Paths.get("/tmp/f"))
        //    : c.replacing(Paths.get("/NULL"));
        //  client.sendAsync(request, bodyHandler)
        //    .thenApply(HttpResponse::body)
        //    .thenAccept(System.out::println);
        // check result
        @Override
        public BodySubscriber<Void> apply(ResponseInfo responseInfo) {
            if (responseInfo.statusCode() == 200) {
                return BodyHandlers.fromSubscriber(this).apply(responseInfo);
            } else {
                return BodyHandlers.discarding().apply(responseInfo);
            }
        }

        /**
         * Handle response
         */
        @Override
        public void onComplete() {
            // assert checksum is ok
            if (!checkReceivedData()) {
                failed = true;
            } else
            try {
                renameFileAfterDownload();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        /**
         * Error occurred while parsing response
         */
        @Override
        public void onError(Throwable err) {
            try {
                deleteBadDownload();
                onError();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                System.exit(1);
            }
        }

        QueableCommand command;

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            for (var buf : buffers) {
                updateDownloadStats(buf);
                if (command == null) {
                    // assert directory creation
                    try {
                        FutureMkdirCommand.create(executor, dirname, identity).get();
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                    try {
                        command = QueableCreateCommand.submit(executor,
                            downloadPath,
                            identity,
                            BufferManager.getFakeWrapper(buf)
                        );
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                } else {
                    command = command.enqueue(BufferManager.getFakeWrapper(buf));
                }
            }
        }

        private long receiveSize = 0;
        private MessageDigest hasher;
        private void updateDownloadStats(ByteBuffer buffer) {
            // get size of available data
            receiveSize += buffer.remaining();
            // update hash
            hasher.update(buffer.asReadOnlyBuffer());
        }

        @Override
        public void onSubscribe(Subscription sub) {
            receiveSize = 0;
            // download started, init checksum fields
            try {
                hasher = MessageDigest.getInstance(fileToDownload.getBestVersion().getHashAlgorithm());
                sub.request(Long.MAX_VALUE);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        /**
         * Checksum and file check
         */
        private boolean checkReceivedData() {
            try {
                if (!command.getFuture().get()) {
                    return false;
                } else {
                    // are size and hash ok?
                    var v = fileToDownload.getBestVersion();
                    return v.getSize() == receiveSize
                        && Arrays.equals(v.getFileHash(), hasher.digest());
                }
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }

    }
}
