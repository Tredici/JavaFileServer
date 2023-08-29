package it.sssupserver.app.handlers.simplecdnhandler;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;

import com.google.gson.GsonBuilder;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
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
        return thisnodeAsJson(true);
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

        // return list of empty directories
        public List<Node> findEmptyDirectories() {
            return snapshot.filter(n -> n.isDirectory() && n.countChildren() == 0);
        }

        public Node[] getAllNodes() {
            return snapshot.getAllNodes();
        }

        // test
        private void assertValidNames() {
            var badFiles = Arrays.stream(snapshot.getAllNodes())
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

        // calculate file hashes
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
            assertValidNames();
            // calculate file hashes
            calculateFileHashes();

            // put snapshot inside a mutable structure
            // in order to facilitate upload/deletion
            // operations

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

        // configuration is read inside constructor - so it is handled at startup!
        // discover owned files
        fsStatus = new ManagedFileSystemStatus();

        // start manager endpoint
        startManagementEndpoint();

        // start join protocol:
        //  discover topology
        //  find previous node
        //  start replication strategy

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
                        var requestedFile = exchange.getRequestURI().getPath();
                        // check if current node is file supplier or redirect
                        if (SimpleCDNHandler.this.testSupplyabilityOrRedirect(requestedFile, exchange)) {
                            try {
                                // recicle old working code
                                HttpSchedulableReadCommand.handle(SimpleCDNHandler.this.executor, exchange, SimpleCDNHandler.this.identity);
                            } catch (Exception e) { System.err.println(e); e.printStackTrace(); }
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

        private void sendIdentity(HttpExchange exchange) throws IOException {
            var me = thisnodeAsJson();
            sendJson(exchange, me);
        }

        private void sendLocalStorageInfo(HttpExchange exchange) throws IOException {
            var me = fsStatus.regularFilesInfoToJson();
            sendJson(exchange, me);
        }

        private void sendFileStatistics(HttpExchange exchange) throws IOException {
            var me = clientStatsCollector.asJson();
            sendJson(exchange, me);
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
                                sendTopology(exchange);
                            }
                            else if (path.equals("identity")) {
                                // return topology
                                sendIdentity(exchange);
                            }
                            else if (path.equals("storage")) {
                                // return topology
                                sendLocalStorageInfo(exchange);
                            }
                            else if (path.equals("stats")) {
                                // return file statistics
                                sendFileStatistics(exchange);
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

        @Override
        public void run() {
            // TODO Auto-generated method stub
            super.run();
        }

    }
    private ControlThread controlThread;

    private void startControlThread() {
        if (controlThread != null) {
            throw new RuntimeException("Control thread already started");
        }
        // ...
        controlThread = new ControlThread();
        controlThread.start();
    }

    private void stopControlThread() {
        if (controlThread == null) {
            throw new RuntimeException("Control thread not started");
        }
        // ...
        controlThread.interrupt();
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
