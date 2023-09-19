package it.sssupserver.app.handlers.simplecdnhandler;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import com.google.gson.GsonBuilder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.util.List;
import java.util.Map;

import it.sssupserver.app.base.BufferManager;
import it.sssupserver.app.base.FileTree;
import it.sssupserver.app.base.FileTree.Node;
import it.sssupserver.app.commands.utils.FileReducerCommand;
import it.sssupserver.app.commands.utils.FutureDeleteCommand;
import it.sssupserver.app.commands.utils.FutureFileSizeCommand;
import it.sssupserver.app.commands.utils.FutureMkdirCommand;
import it.sssupserver.app.commands.utils.FutureMoveCommand;
import it.sssupserver.app.commands.utils.ListTreeCommand;
import it.sssupserver.app.commands.utils.QueableCreateCommand;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.handlers.RequestHandler;
import it.sssupserver.app.handlers.httphandler.HttpHelper;
import it.sssupserver.app.handlers.simplecdnhandler.gson.*;
import it.sssupserver.app.handlers.simplecdnhandler.httphandlers.ApiManagementHttpHandler;
import it.sssupserver.app.handlers.simplecdnhandler.httphandlers.ClientHttpHandler;
import it.sssupserver.app.handlers.simplecdnhandler.httphandlers.FileManagementHttpHandler;
import it.sssupserver.app.handlers.simplecdnhandler.pathhashers.DefaultPathHasher;
import it.sssupserver.app.handlers.simplecdnhandler.pathhashers.PathHasher;
import it.sssupserver.app.users.Identity;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpClient.Redirect;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.tika.Tika;

import static it.sssupserver.app.handlers.httphandler.HttpResponseHelpers.*;
import static it.sssupserver.app.handlers.simplecdnhandler.FilenameCheckers.*;
import static it.sssupserver.app.base.HexUtils.*;

public class SimpleCDNHandler implements RequestHandler {

    // by default this is the name of the configuration file, contained inside the CWD
    public static final String DEFAULT_CONFIG_FILE = "SimpleCDN.json";
    private Path config_file = Path.of(DEFAULT_CONFIG_FILE);

    public static final int BACKLOG = 1024;

    // running configuration loaded from comfig file inside constructor
    private SimpleCDNConfiguration config;
    // identity associated with current replica
    private Identity identity;

    public Identity getIdentity() {
        return identity;
    }

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
     * Directories cannot contains '.' (dot), substitute them with '#'
     */
    public static it.sssupserver.app.base.Path sanitazePath(it.sssupserver.app.base.Path path) {
        it.sssupserver.app.base.Path ans;
        if (path.isDir()) {
            // refer to dir
            ans = new it.sssupserver.app.base.Path(
                path.toString().replaceAll("\\.", "#"), true
            );
        } else {
            // regular file
            ans = new it.sssupserver.app.base.Path(
                path.getDirname().toString().replaceAll("\\.", "#"), true
            ).createSubfile(
                path.getBasename().contains(".")
                ? path.getBasename()
                : "#" + path.getBasename()
            );
        }
        return ans;
    }
    /**
     * Revert sanitazePath operations
     */
    public static it.sssupserver.app.base.Path unsanitazePath(it.sssupserver.app.base.Path path) {
        it.sssupserver.app.base.Path ans;
        if (path.isDir()) {
            // refer to dir
            ans = new it.sssupserver.app.base.Path(
                path.toString().replaceAll("#", "\\."), true
            );
        } else {
            // regular file
            ans = new it.sssupserver.app.base.Path(
                path.getDirname().toString().replaceAll("#", "\\."), true
            ).createSubfile(
                path.getBasename().startsWith("#")
                ? path.getBasename().substring(1)
                : path.getBasename()
            );
        }
        return ans;
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
        return o.getDataEndpoints().length > 0
         && o.getManagementEndpoints().length > 0
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

    public String thisnodeAsJson() {
        return thisnodeAsJson(false);
    }

    private URL[] getClientEndpoints() {
        return thisnode.dataEndpoints;
    }

    private URL[] getManagementEndpoints() {
        return thisnode.managementEndpoints;
    }

    // Estimate MIME Type from file name
    public String estimateMimeType(String filename) {
        return HttpHelper.estimateMimeType(filename);
    }

    // hold topology seen by this node
    private Topology topology;

    private PathHasher pathHasher = new DefaultPathHasher();
    public long hashFilePath(String path) {
        return pathHasher.hashFilename(path);
    }

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
        var hash = hashFilePath(path);
        if (topology.isFileOwned(hash)) {
            return true;
        } else {
            var owner = topology.getFileOwner(hash);
            var index = (int)(owner.dataEndpoints.length * Math.random());
            var redirect = owner.dataEndpoints[index];
            try {
                // Set Location header
                //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Location
                exchange.getResponseHeaders()
                    .add("Location", redirect.toURI()
                    .resolve(path.replaceAll(" ", "%20")).toString());
                // 308 Permanent Redirect
                //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/308
                exchange.sendResponseHeaders(308, 0);
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
        var hash = hashFilePath(path);
        if (topology.isFileSupplier(hash)) {
            return true;
        } else {
            var owner = topology.peekRandomSupplier(hash);
            var redirect = owner.getRandomDataEndpointURL();
            try {
                // 308 Permanent Redirect
                //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Redirections
                exchange.getResponseHeaders()
                    .add("Location", redirect.toURI()
                    .resolve(path.replaceAll(" ", "%20")).toString());
                exchange.sendResponseHeaders(308, 0);
                exchange.getResponseBody().flush();
                exchange.close();
            } catch (Exception e) {
                // TODO: should only log error
                e.printStackTrace();
            }
            return false;
        }
    }

    public boolean testOwnershipOrRedirectToManagement(String path, HttpExchange exchange) {
        var hash = hashFilePath(path);
        if (topology.isFileOwned(hash)) {
            return true;
        } else {
            var owner = topology.getFileOwner(hash);
            var redirect = owner.getRandomManagementEndpointURL();
            try {
                // Set Location header
                //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Location
                exchange.getResponseHeaders()
                    .add("Location", redirect.toURI()
                    .resolve(FileManagementHttpHandler.PATH + "/")
                    .resolve(path.replaceAll(" ", "%20")).toString());
                // 308 Permanent Redirect
                //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/308
                exchange.sendResponseHeaders(308, 0);
                exchange.getResponseBody().flush();
                // help .NET that is unable to cope with response-before-request
                if (exchange.getRequestMethod().equals("PUT")) {
                    exchange.getRequestBody().transferTo(OutputStream.nullOutputStream());
                }
                exchange.close();
            } catch (Exception e) {
                e.printStackTrace();
                httpInternalServerError(exchange);
            }
            return false;
        }
    }

    public boolean testSupplyabilityOrRedirectToManagement(String path, HttpExchange exchange) {
        var hash = hashFilePath(path);
        if (topology.isFileSupplier(hash)) {
            return true;
        } else if (path.contains("@")) {
            // if path contains metadata disable redirection
            return true;
        } else {
            var owner = topology.peekRandomSupplier(hash);
            var redirect = owner.getRandomManagementEndpointURL();
            try {
                exchange.getResponseHeaders()
                    .add("Location", redirect.toURI()
                    .resolve(FileManagementHttpHandler.PATH + "/")
                    .resolve(path.replaceAll(" ", "%20")).toString());
                exchange.sendResponseHeaders(308, 0);
                exchange.getResponseBody().flush();
                exchange.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return false;
        }
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
                        var firstLine = FileReducerCommand.reduce(executor, sanitazePath(getPath()), identity, accumulator, reducer, finalizer).toString();
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
                            var cc = QueableCreateCommand.submit(executor, sanitazePath(dstPath), identity, w);
                            if (cc.getFuture().get() != true) {
                                throw new RuntimeException("Failed to create file: " + searchPath);
                            }
                        }
                        // delete original one
                        var f = FutureDeleteCommand.delete(executor, sanitazePath(currentPath), identity);
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
                // enforce directory creation
                FutureMkdirCommand.create(executor, sanitazePath(dstPath.getDirname()), identity).get();
                // schedule creation of file
                var cc = QueableCreateCommand.submit(executor, sanitazePath(dstPath), identity, w);
                if (cc.getFuture().get() != true) {
                    throw new RuntimeException("Failed to add deleted file: " + searchPath);
                }
                // add new node
                var newNode = addRegularFileNode(dstPath);
                newNode.setSize(deleted.getBestVersion().getSize());
                newNode.setFileHash(deleted.getBestVersion().getHashAlgorithm(), deleted.getBestVersion().getFileHash());
                var lfi = fsStatus.addLocalFileInfo(newNode);
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
                var f = FutureMoveCommand.move(executor, sanitazePath(filePath), sanitazePath(dstPath), identity);
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
                var f = FutureMoveCommand.move(executor, sanitazePath(filePath), sanitazePath(dstPath), identity);
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

        /**
         * Replace "." (dots) with "#" in file names,
         * used to avoid clashes in
         */
        private void unsanitazePathNames() {
            Arrays.stream(snapshot.getAllNodes())
                .forEach(
                    n -> {
                        n.setPath(
                            unsanitazePath(
                                n.getPath()
                            )
                        );
                    }
                );
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
                    return FileReducerCommand.reduceByHash(executor, sanitazePath(filename), identity, HASH_ALGORITHM);
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
                    return FutureFileSizeCommand.querySize(executor, sanitazePath(filename), identity);
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
            // unsanitaze all names to recover original names
            unsanitazePathNames();
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

    public ManagedFileSystemStatus getFsStatus() {
        return fsStatus;
    }

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
                        var hash = hashFilePath(searchPath);
                        // check if already downloaded in the meantime
                        if (handler.getFsStatus().isRemoteVersionDownloadable(file)
                            // or if topology has changed
                            && topology.isFileSupplier(hash)
                        ) {
                            startDownloadOrDeletion(searchPath, file);
                        } else {
                            // release lock
                            locks.remove(searchPath);
                        }
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
            var hash = hashFilePath(searchPath);
            if (topology.isFileSupplier(hash)) {
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

    public FileManager getFileManager() {
        return executor;
    }

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
        thisnode.dataEndpoints = Arrays.stream(config.getClientEndpoints())
            .map(he -> he.getUrl()).toArray(URL[]::new);
        thisnode.managementEndpoints = Arrays.stream(config.getManagementEndpoints())
            .map(he -> he.getUrl()).toArray(URL[]::new);
        thisnode.replicationFactor = config.getReplicationFactor();
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

    // this object is mantained alive for all the Handler lifespan
    private StatsCollector clientStatsCollector = new StatsCollector();

    public void incrementDownloadCount(String searchPath) {
        var s = clientStatsCollector.getFileStats(searchPath);
        if (s != null) {
            s.incSupplied();
        }
    }

    public void incrementRedirectCount(String searchPath) {
        var s = clientStatsCollector.getFileStats(searchPath);
        if (s != null) {
            s.incRedirects();
        }
    }

    public void incrementNotFoundCount(String searchPath) {
        var s = clientStatsCollector.getFileStats(searchPath);
        if (s != null) {
            s.incErrors();
        }
    }

    public String regularFilesInfoWithMeToJson(boolean prettyPrinting) {
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

    public String topologyWithMeToJson(boolean prettyPrinting) {
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

    public String suppliableFilesWithIdentity(boolean prettyPrinting, boolean detailed) {
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
            .registerTypeAdapter(DataNodeDescriptor.class, new DataNodeDescriptorGson())
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
            var hs = HttpServer.create(addr, BACKLOG);
            // Add http handler for clients
            hs.createContext("/", new ClientHttpHandler(this));
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

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Operations associated with management endpoints ++++++++++++++++
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

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
            .resolve(searchPath.replaceAll(" ", "%20"));
        var req = HttpRequest.newBuilder(rUri)
            .GET()
            .build();
        return req;
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
            var hs = HttpServer.create(addr, BACKLOG);
            // handle /api calls
            hs.createContext(ApiManagementHttpHandler.PATH, new ApiManagementHttpHandler(this));
            // handle GET, PUT and DELETE for admins only
            hs.createContext(FileManagementHttpHandler.PATH, new FileManagementHttpHandler(this));
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

    public void watchIfNotWatched(DataNodeDescriptor remoteNode) throws URISyntaxException {
        var rn = topology.findDataNodeDescriptorById(remoteNode.getId());
        if (rn == null && isDataNodeCompatible(remoteNode)) {
            for (var me : remoteNode.getManagementEndpoints()) {
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
        if (peer == null) {
            // prevent unexpected race condition
            return;
        }
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
        if (peer.getStatus() == DataNodeDescriptor.Status.FAILED) {
            // remote node FAILED! Maybe resync might help in case of fast failures
            candidatesById.forEach((k, v) -> v.requestFileCheck());
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
                    for (var url : peer.getManagementEndpoints()) {
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

    // ----------------------------------------------------------------
    // Operations associated with control thread ----------------------
    // ----------------------------------------------------------------

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
            this,
            file,
            callback
        );
        scheduleDownload(downloader);
    }

    protected void scheduleDownload(FileDownloader downloader) {
        // schedule download
        getThreadPool().submit(downloader);
    }

}
