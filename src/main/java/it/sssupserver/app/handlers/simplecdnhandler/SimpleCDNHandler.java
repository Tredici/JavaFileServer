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

import it.sssupserver.app.base.FileTree;
import it.sssupserver.app.base.FileTree.Node;
import it.sssupserver.app.commands.utils.FileReducerCommand;
import it.sssupserver.app.commands.utils.FutureFileSizeCommand;
import it.sssupserver.app.commands.utils.FutureMoveCommand;
import it.sssupserver.app.commands.utils.ListTreeCommand;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.handlers.RequestHandler;
import it.sssupserver.app.handlers.httphandler.HttpSchedulableReadCommand;
import it.sssupserver.app.handlers.simplecdnhandler.SimpleCDNConfiguration.HttpEndpoint;
import it.sssupserver.app.users.Identity;

import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


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

    // this class
    private static class DataNodeDescriptor {
        // a DataNote can traverse all these status
        public enum Status {
            // node not working
            SHUTDOWN,
            // normal running status, traversed in this order
            STARTING,
            SYNCING,
            RUNNING,
            STOPPING,
            // error status
            MAYBE_FAILED,
            FAILED
        }
        // id identifing the node
        public long id;
        // list of: http://myendpoint:port
        // used to find http endpoints to download data (as client)
        public URL[] dataendpoints;
        // used to find http endpoint to operate consistency protocol
        public URL[] managerendpoint;
        // how many replicas for each file? Default: 3
        public int replication_factor = 3;
        // status of the node
        public Status status = Status.SHUTDOWN;
    }

    public static class DataNodeDescriptorGson implements JsonSerializer<DataNodeDescriptor> {
        @Override
        public JsonElement serialize(DataNodeDescriptor src, Type typeOfSrc, JsonSerializationContext context) {
            var jObj = new JsonObject();
            jObj.add("Id", new JsonPrimitive(src.id));
            jObj.add("ReplicationFactor", new JsonPrimitive(src.replication_factor));
            jObj.add("DataEndpoints", context.serialize(src.dataendpoints));
            jObj.add("ManagerEndpoint", context.serialize(src.managerendpoint));
            jObj.addProperty("Status", src.status.toString());
            return jObj;
        }
    }

    // hold informations about current instance
    DataNodeDescriptor thisnode;

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

    // this class will hold all the informations about
    // the ring topology
    private class Topology {
        private ConcurrentSkipListMap<Long, DataNodeDescriptor> datanodes = new ConcurrentSkipListMap<>();

        // default topology is only me
        public Topology() {
            if (thisnode == null) {
                throw new RuntimeException("SimpleCDNHandler.this.thisnode must not be null!");
            }
            datanodes.put(thisnode.id, thisnode);
        }

        // null means thisnode
        public DataNodeDescriptor findPrevious(DataNodeDescriptor node) {
            // any previous?
            var prevE = datanodes.floorEntry(node.id-1);
            if (prevE == null) {
                // search for last
                prevE = datanodes.lastEntry();
            }
            // return other or me
            return prevE.getValue() == node ? null : prevE.getValue();
        }

        // find previous of the current node,
        // return null if current node is alone
        // (i.e. it is its own successor)
        public DataNodeDescriptor findPrevious() {
            // id of current node
            var me = SimpleCDNHandler.this.thisnode;
            // any previous?
            return findPrevious(me);
        }

        // null means thisnode
        public DataNodeDescriptor findSuccessor(DataNodeDescriptor node) {
            // any successor?
            var succE = datanodes.ceilingEntry(node.id+1);
            if (succE == null) {
                // search for last
                succE = datanodes.firstEntry();
            }
            // return other or me
            return succE.getValue() == node ? null : succE.getValue();
        }

        // find successor of the current node,
        // return null if current node is alone
        // (i.e. it is its own successor)
        public DataNodeDescriptor findSuccessor() {
            // id of current node
            var me = SimpleCDNHandler.this.thisnode;
            // any successor?
            return findSuccessor(me);
        }

        // get hash from file name
        public long getFileHash(String path) {
            return (long)path.hashCode() << 32;
        }

        public DataNodeDescriptor getFileOwner(String path) {
            // get file hash
            var hash = getFileHash(path);
            var ownerE = datanodes.ceilingEntry(hash);
            if (ownerE == null) {
                ownerE = datanodes.lastEntry();
            }
            return ownerE.getValue();
        }

        public List<DataNodeDescriptor> getFileSuppliers(String path) {
            // expected number o
            var R = SimpleCDNHandler.this.thisnode.replication_factor;
            List<DataNodeDescriptor> ans = new ArrayList<>(R);
            final var owner = getFileOwner(path);
            ans.add(owner);
            var supplier = owner;
            for (int i=1; i<R; ++i) {
                supplier = findPrevious(supplier);
                if (supplier == null || ans.contains(supplier)) {
                    // avoid looping - this strategy (i.e. check all instead of checking
                    // if owner refound) is ok also in case of concurrent topology changes
                    break;
                }
                ans.add(supplier);
            }
            return ans;
        }

        // obtain random supplier to perform redirect
        public DataNodeDescriptor peekRandomSupplier(String path) {
            var candidates = getFileSuppliers(path);
            var node = candidates.get((int)(candidates.size() * Math.random()));
            return node;
        }

        // is given node supplier of file?
        public boolean isFileSupplier(DataNodeDescriptor node, String path) {
            return getFileSuppliers(path).contains(node);
        }

        // is current node supplier of file?
        public boolean isFileSupplier(String path) {
            return isFileSupplier(SimpleCDNHandler.this.thisnode, path);
        }

        // Does given datanode own specified file?
        public boolean isFileOwner(DataNodeDescriptor datanode, String path) {
            return datanode == getFileOwner(path);
        }

        // is the file owned by the current node?
        public boolean isFileOwned(String path) {
            return SimpleCDNHandler.this.thisnode == getFileOwner(path);
        }

        public DataNodeDescriptor[] getSnapshot() {
            return datanodes.values().toArray(new DataNodeDescriptor[0]);
        }

        public String asJsonArray(boolean prettyPrinting) {
            var snapshot = getSnapshot();
            var gBuilder = new GsonBuilder();
            if (prettyPrinting) {
                gBuilder = gBuilder.setPrettyPrinting();
            }
            var gson = gBuilder
                .registerTypeAdapter(DataNodeDescriptor.class, new DataNodeDescriptorGson())
                .create();
            var jSnapshot = gson.toJson(snapshot);
            return jSnapshot;
        }

        public String asJsonArray() {
            return asJsonArray(false);
        }
    }

    // hold topology seen by this node
    private Topology topology;

    // respond with redirect message for supplied file
    // return true if current node own specified file
    // NOTE: test OWNERSHIP, not ability to supply it!
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

    // initialize parameters used to check validity of file path(s)
    private static Predicate<String> directoryNameTest;
    private static Predicate<String> regularFileNameTest;
    static {
        directoryNameTest = Pattern.compile(
            "^(_|-|\\+|\\w)+$",
            Pattern.CASE_INSENSITIVE
        ).asMatchPredicate();
        regularFileNameTest = Pattern.compile(
            "^(_|-|\\+|\\w)+(\\.(_|-|\\+|\\w)+)+$",
            Pattern.CASE_INSENSITIVE
        ).asMatchPredicate();
    }

    // pattern used to insert metadata informations inside file names
    private static Pattern fileMetadataPattern;
    static {
        fileMetadataPattern = Pattern.compile("^(?<simpleName>[^@]*)(@(?<timestamp>\\d+))?(?<temporary>@tmp)?(?<corrupted>@corrupted)?$");
    }

    // directories should match "^(_|-|\+|\w)+$"
    // Easy: they must NOT contains any dot
    private static boolean isValidDirectoryName(String dirname) {
        return dirname.length() > 0 && directoryNameTest.test(dirname);
    }

    // directories should match "^(_|-|\+|\w)+(\.(_|-|\+|\w)+)+$"
    private static boolean isValidRegularFileName(String filename) {
        return filename.length() > 0 && regularFileNameTest.test(filename);
    }

    public static boolean isValidPathName(it.sssupserver.app.base.Path path) {
        if (path.isEmpty()) {
            return false;
        }
        var isDir = path.isDir();
        var components = path.getPath();
        var length = components.length;
        var lastName = components[length-1];
        for (int i=0; i<length-1; ++i) {
            if (!isValidDirectoryName(components[i])) {
                return false;
            }
        }
        return isDir ? isValidDirectoryName(lastName)
            : isValidRegularFileName(lastName);
    }

    public static boolean isValidPathName(String path) {
        return isValidPathName(new it.sssupserver.app.base.Path(path));
    }


    private class NodeGson implements JsonSerializer<Node> {
        @Override
        public JsonElement serialize(Node src, Type typeOfSrc, JsonSerializationContext context) {
            var jObj = new JsonObject();
            jObj.addProperty("Path", src.getPath().toString());
            jObj.addProperty("Size", src.getSize());
            jObj.addProperty("HashAlgorithm", src.getHashAlgorithm());
            jObj.addProperty("Hash", bytesToHex(src.getFileHash()));
            return jObj;
        }
    }

    /**
     * This class maintains a view of the files
     * managed by this DataNode.
     * It should be re-initialized every time this
     * handler is started.
     */
    private class ManagedFileSystemStatus {
        // hash algorithm chosen to compare files owned by replicas
        public static final String HASH_ALGORITHM = FileReducerCommand.MD5;

        // Maintain a view of the file tree managed by this replica node
        FileTree snapshot;

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
                // associate file with Node olding its properties
                private FileTree.Node node;

                public Version(FileTree.Node node, boolean isCorrupted, boolean isTemporary) {
                    this.node = node;
                    this.corrupted = isCorrupted;
                    this.tmp = isTemporary;

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

                /**
                 * A file can be supplied to clients if:
                 *  it is not temporary
                 *  it is not corrupted
                 */
                public boolean isSuppliable() {
                    return !isCorrupted() && !isTmp();
                }
            }

            public Version addVersion(FileTree.Node node, FilenameMetadata metadata) {
                var ans = new Version(node, metadata.isCorrupted(), metadata.isTemporary());
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
                                .compareTo(candidate.getLastUpdateTimestamp()) < 0) {
                            ans = candidate;
                        }
                    }
                }
                return ans;
            }
        }

        // Concurrent map of local available files
        private ConcurrentSkipListMap<String, LocalFileInfo> localFiles = new ConcurrentSkipListMap<>();

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
            if (v == null) {
                return null;
            }
            return v.getPath();
        }

        // Helper class used to parse true path names in order to
        // extract
        private class FilenameMetadata {
            // Filename structure:
            //  "{regular file name character}
            //   (@\d: timestamp)?
            //   (@corrupted)?
            //   (@tmp)?"

            private String simpleName;
            private Instant timestamp;
            private boolean corrupted;
            private boolean temporary;

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
                if (time != null) {
                    this.timestamp = Instant.ofEpochMilli(Long.parseLong(time));
                }
                if (corr != null) {
                    this.corrupted = true;
                }
                if (tmp != null) {
                    this.temporary = true;
                }
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

            public String toString() {
                var sb = new StringBuffer();
                sb.append(simpleName);
                if (timestamp != null) {
                    sb.append('@').append(timestamp.toEpochMilli());
                }
                if (isTemporary()) {
                    sb.append("@tmp");
                }
                if (isCorrupted()) {
                    sb.append("@corrupted");
                }
                return sb.toString();
            }
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
            // add new version
            var version = lfi.addVersion(node, metadata);
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

    /**
     * Gson serializer for LocalFileInfo.Version
     * When serializing, present only the newest version,
     * others are only for internal usage
     */
    public class LocalFileVersionGson implements JsonSerializer<ManagedFileSystemStatus.LocalFileInfo> {
        private boolean detailed = false;

        public LocalFileVersionGson() {
        }

        public LocalFileVersionGson(boolean printDetailed) {
            detailed = printDetailed;
        }

        @Override
        public JsonElement serialize(ManagedFileSystemStatus.LocalFileInfo src, Type typeOfSrc, JsonSerializationContext context) {
            var jObj = new JsonObject();
            jObj.addProperty("SearchPath", src.getSearchPath());
            if (!detailed) {
                var v = src.getLastSuppliableVersion();
                jObj.addProperty("Size", v.getSize());
                jObj.addProperty("HashAlgorithm", v.getHashAlgotrithm());
                jObj.addProperty("Hash", bytesToHex(v.getFileHash()));
                jObj.addProperty("LastUpdated", v.getLastUpdateTimestamp().toEpochMilli());
            } else {
                var versions = src.getAllVersions();
                var jArray = new JsonArray(versions.length);
                for (var v : versions) {
                    var vObj = new JsonObject();
                    vObj.addProperty("Size", v.getSize());
                    vObj.addProperty("HashAlgorithm", v.getHashAlgotrithm());
                    vObj.addProperty("Hash", bytesToHex(v.getFileHash()));
                    vObj.addProperty("LastUpdated", v.getLastUpdateTimestamp().toEpochMilli());
                    vObj.addProperty("RealPath", v.getPath().toString());
                    jArray.add(vObj);
                }
                jObj.add("Versions", jArray);
            }
            return jObj;
        }
    }

    private ManagedFileSystemStatus fsStatus;

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
        topology = new Topology();
    }

    // rely on a thread pool in order to .
    private ExecutorService threadPool;

    @Override
    public void start() throws Exception {
        if (threadPool != null) {
            throw new RuntimeException("Handler already started");
        }
        threadPool = Executors.newCachedThreadPool();

        // UTC start time
        startInstant = Instant.now();

        // configuration is read inside constructor - so it is handled at startup!
        // discover owned files
        fsStatus = new ManagedFileSystemStatus();

        // start manager endpoint
        startManagementEndpoint();

        // start join protocol:
        //  discover topology
        //  find previous node
        //  start replication strategy
        startControlThread();


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

        // start exit protocol
        // shutdown manager endpoint
        stopManagementEndpoint();

        // shutdown client endpoint
        // this is the last stage in order to reduce
        // service disruption to clients
        stopClientEndpoints();

        threadPool.shutdown();
        threadPool = null;

        // TODO Auto-generated method stub
        //throw new UnsupportedOperationException("Unimplemented method 'stop'");
    }

    /**
     * This class will be charged of collecting and supplying
     * all all statistics regarding client operations affecting
     * this node
     */
    private class StatsCollector {

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

    public static class FileStatsGson implements JsonSerializer<StatsCollector.FileStats> {
        @Override
        public JsonElement serialize(StatsCollector.FileStats src, Type typeOfSrc, JsonSerializationContext context) {
            var jObj = new JsonObject();
            jObj.addProperty("Path", src.getPath());
            jObj.addProperty("Supplied", src.getSupplied());
            jObj.addProperty("Redirects", src.getRedirects());
            jObj.addProperty("Errors", src.getErrors());
            return jObj;
        }
    }

    public static class StatsCollectorGson implements JsonSerializer<StatsCollector> {
        @Override
        public JsonElement serialize(StatsCollector src, Type typeOfSrc, JsonSerializationContext context) {
            var stats = src.getFileStats();
            return context.serialize(stats);
        }
    }

    public class FileInfoWithIdentityGson implements JsonSerializer<FileInfoWithIdentityGson> {
        private DataNodeDescriptor identity;
        private FileTree.Node[] nodes;

        public FileInfoWithIdentityGson(DataNodeDescriptor identity, FileTree.Node[] nodes) {
            this.identity = identity;
            this.nodes = nodes;
        }

        @Override
        public JsonElement serialize(FileInfoWithIdentityGson src, Type typeOfSrc, JsonSerializationContext context) {
            var jObj = new JsonObject();
            jObj.add("Identity", context.serialize(identity));
            jObj.add("LocalFiles", context.serialize(nodes));
            return jObj;
        }
    }

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

    public class TopologyWithIdentityGson implements JsonSerializer<TopologyWithIdentityGson> {
        private DataNodeDescriptor identity;
        private DataNodeDescriptor[] snapshot;

        public TopologyWithIdentityGson(DataNodeDescriptor identity, DataNodeDescriptor[] snapshot) {
            this.identity = identity;
            this.snapshot = snapshot;
        }

        @Override
        public JsonElement serialize(TopologyWithIdentityGson src, Type typeOfSrc, JsonSerializationContext context) {
            var jObj = new JsonObject();
            jObj.add("Identity", context.serialize(identity));
            jObj.add("Topology", context.serialize(snapshot));
            return jObj;
        }
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

    public class SuppliableFilesWithIdentityGson implements JsonSerializer<SuppliableFilesWithIdentityGson> {
        private DataNodeDescriptor identity;
        private ManagedFileSystemStatus.LocalFileInfo[] localFileInfos;

        public SuppliableFilesWithIdentityGson(DataNodeDescriptor identity, ManagedFileSystemStatus.LocalFileInfo[] localFileInfos) {
            this.identity = identity;
            this.localFileInfos = localFileInfos;
        }

        @Override
        public JsonElement serialize(SuppliableFilesWithIdentityGson src, Type typeOfSrc, JsonSerializationContext context) {
            var jObj = new JsonObject();
            jObj.add("Identity", context.serialize(identity));
            jObj.add("SuppliableFiles", context.serialize(localFileInfos));
            return jObj;
        }
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

    public class StatisticsWithIdentityGson implements JsonSerializer<StatisticsWithIdentityGson> {
        private DataNodeDescriptor identity;
        private StatsCollector.FileStats[] statistics;

        public StatisticsWithIdentityGson(DataNodeDescriptor identity, StatsCollector.FileStats[] statistics) {
            this.identity = identity;
            this.statistics = statistics;
        }

        @Override
        public JsonElement serialize(StatisticsWithIdentityGson src, Type typeOfSrc, JsonSerializationContext context) {
            var jObj = new JsonObject();
            jObj.add("Identity", context.serialize(identity));
            jObj.add("Statistics", context.serialize(statistics));
            return jObj;
        }
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

    private void methodNotAllowed(HttpExchange exchange) {
        try {
            // 405 Method Not Allowed
            //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/405
            exchange.sendResponseHeaders(405, 0);
            exchange.getResponseBody().flush();
            exchange.close();
        } catch (Exception e) { System.err.println(e); e.printStackTrace(); }
    }

    private void httpNotFound(HttpExchange exchange) {
        try {
            exchange.sendResponseHeaders(404, 0);
            exchange.getResponseBody().flush();
            exchange.close();
        } catch (Exception e) { System.err.println(e); e.printStackTrace(); }
    }

    private void httpInternalServerError(HttpExchange exchange) {
        try {
            exchange.sendResponseHeaders(500, 0);
            exchange.getResponseBody().flush();
            exchange.close();
        } catch (Exception e) { System.err.println(e); e.printStackTrace(); }
    }

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Operations associated with client endpoints ++++++++++++++++++++
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    class ClientHttpHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                switch (exchange.getRequestMethod()) {
                    case "GET":
                        var requestedFile = exchange.getRequestURI().getPath().substring(1);
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
                        methodNotAllowed(exchange);
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
            var info = suppliableFilesWithIdentity(false, false);
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

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                switch (exchange.getRequestMethod()) {
                    case "GET":
                        // retrieve node or topology information
                        {
                            var path = new URI(PATH).relativize(exchange.getRequestURI()).getPath();
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
                            else {
                                httpNotFound(exchange);
                            }
                        }
                        break;
                    case "POST":
                        // for join operations and so...
                        break;
                    default:
                        methodNotAllowed(exchange);
                }
            } catch (Exception e) {
                System.err.println(e);
                e.printStackTrace();
            }
        }

    }
    // listen for PUT and DELETE commands
    private class FileManagementHttpHandler implements HttpHandler {
        public static final String PATH = "/file";

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                switch (exchange.getRequestMethod()) {
                    case "GET":
                        // download file - same as for users
                        break;
                    case "DELETE":
                        // delete file - handle deletion protocol
                        break;
                    case "PUT":
                        // upload - handle replication protocol
                        break;
                    default:
                        methodNotAllowed(exchange);
                }
            } catch (Exception e) {
                System.err.println(e);
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

    private class ControlThread extends Thread {

        // look for candidate DataNode(s) to connect to
        private void handleJoinProtocol() {

        }

        // start exit protocol to inform other node of intent
        // to leave the ring
        private void handleExitProtocol() {

        }

        private void handleRunningStatus() {

        }

        @Override
        public void run() {
            handleJoinProtocol();
            handleRunningStatus();
            handleExitProtocol();
        }

        /**
         * Before:
         *  current node status must be DataNodeDescriptor.Status.SHUTDOWN
         * After:
         *  current node status must be DataNodeDescriptor.Status.RUNNING
         */
        public void waitForStartingProtocolCompletion() {
            if (thisnode.status != DataNodeDescriptor.Status.SHUTDOWN) {
                throw new RuntimeException("Node status is not SHUTDOWN: " + thisnode.status);
            }

            ;
        }

        /**
         * Before:
         *  current node status must be DataNodeDescriptor.Status.RUNNING
         * After:
         *  current node status must be DataNodeDescriptor.Status.SHUTDOWN
         */
        public void waitForStoppingProtocolCompletion() {
            if (thisnode.status != DataNodeDescriptor.Status.RUNNING) {
                throw new RuntimeException("Node status is not RUNNING: " + thisnode.status);
            }

            ;
        }
    }
    private ControlThread controlThread;

    // start control thread, return when JOIN protocol is terminated
    private void startControlThread() {
        if (controlThread != null) {
            throw new RuntimeException("Control thread already started");
        }
        // ...
        controlThread = new ControlThread();
        controlThread.start();
        controlThread.waitForStartingProtocolCompletion();

    }

    private void stopControlThread() {
        if (controlThread == null) {
            throw new RuntimeException("Control thread not started");
        }
        // ...
        controlThread.interrupt();
        controlThread.waitForStoppingProtocolCompletion();
        try {
            controlThread.join();
        } catch (InterruptedException e) {
            System.err.println(e);
            e.printStackTrace();
        }
        controlThread = null;
    }

    // ----------------------------------------------------------------
    // Operations associated with control thread ----------------------
    // ----------------------------------------------------------------

    // schedule read command to obtain file content and
    // send it to remote node
    private Future sendFileToDataNode(URI remoteDataNode, Path file) {

        return null;
    }

    // Code source:
    //  https://stackoverflow.com/questions/9655181/java-convert-a-byte-array-to-a-hex-string
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toLowerCase().toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }
}
