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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


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
        public long id;
        // list of: http://myendpoint:port
        // used to find http endpoints to download data (as client)
        public URL[] dataendpoints;
        // used to find http endpoint to operate consistency protocol
        public URL[] managerendpoint;
        // how many replicas for each file? Default: 3
        public int replication_factor = 3;
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

    private void methodNotAllowed(HttpExchange exchange) {
        try {
            // 405 Method Not Allowed
            //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/405
            exchange.sendResponseHeaders(405, 0);
            exchange.getResponseBody().flush();
            exchange.close();
        } catch (Exception e) { System.err.println(e); }
    }

    private void httpNotFound(HttpExchange exchange) {
        try {
            exchange.sendResponseHeaders(404, 0);
            exchange.getResponseBody().flush();
            exchange.close();
        } catch (Exception e) { System.err.println(e); }
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
                            } catch (Exception e) { System.err.println(e); }
                        }
                        break;
                    default:
                        methodNotAllowed(exchange);
                        break;
                }
            } catch (Exception e) {
                System.err.println(e);
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

}
