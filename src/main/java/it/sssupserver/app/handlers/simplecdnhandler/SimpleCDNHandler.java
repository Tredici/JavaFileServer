package it.sssupserver.app.handlers.simplecdnhandler;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.util.List;
import java.util.Map;

import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.handlers.RequestHandler;
import it.sssupserver.app.handlers.httphandler.HttpSchedulableReadCommand;
import it.sssupserver.app.users.Identity;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.URIParameter;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;


public class SimpleCDNHandler implements RequestHandler {

    // by default this is the name of the configuration file, contained inside the CWD
    public static final String DEFAULT_CONFIG_FILE = "SimpleCDN.json";
    private Path config_file = Path.of(DEFAULT_CONFIG_FILE);

    // running configuration loaded from comfig file inside constructor
    private SimpleCDNConfiguration config;
    // identity associated with current replica
    private Identity identity;

    // this class
    private class DataNodeDescriptor {
        public long id;
        // list of: http://myendpoint:port
        // used to find http endpoints to download data (as client)
        public URI[] dataendpoints;
        // used to find http endpoint to operate consistency protocol
        public URI[] managerendpoint;
        // how many replicas for each file? Default: 3
        public int replication_strategy = 3;
    }

    // hold informations about current instance
    DataNodeDescriptor thisnode;

    // this class will hold all the informations about
    // the ring topology
    private class Topology {
        private ConcurrentSkipListMap<Long, DataNodeDescriptor> datanodes = new ConcurrentSkipListMap<>();

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
            var R = SimpleCDNHandler.this.thisnode.replication_strategy;
            List<DataNodeDescriptor> ans = new ArrayList<>(R);
            final var owner = getFileOwner(path);
            ans.add(owner);
            var supplier = owner;
            for (int i=1; i<R; ++i) {
                supplier = findPrevious(supplier);
                if (ans.contains(supplier)) {
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
    }

    // TODO: initialize inside constructor
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
                    .add("Location", redirect.resolve(path).toString());
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
                    .add("Location", redirect.resolve(path).toString());
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
        } catch (FileNotFoundException e) {
            System.err.println("Error while parsing configuration file:");
            System.err.println(e);
            System.exit(1);
        }
        // associate identity with current instance
        identity = new Identity(config.getUser());
    }

    @Override
    public void start() throws Exception {

        // configuration is read inside constructor - so it is handled at startup!

        // start manager endpoint

        // start join protocol:
        //  discover topology
        //  find previous node
        //  start replication strategy
        //  start client endpoint

        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'start'");
    }


    @Override
    public void stop() throws Exception {

        // shutdown client endpoint
        // start exit protocol
        // shutdown manager endpoint

        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'stop'");
    }


    // Operations associated with client endpoints ++++++++++++++++++++
    class ClientHttpHandler implements HttpHandler {
        private void methodNotAllowed(HttpExchange exchange) {
            try {
                // 405 Method Not Allowed
                //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/405
                exchange.sendResponseHeaders(405, 0);
                exchange.getResponseBody().flush();
                exchange.close();
            } catch (Exception e) { }
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            switch (exchange.getRequestMethod()) {
                case "GET":
                    var requestedFile = exchange.getRequestURI().toURL().getPath();
                    // check if current node is file supplier or redirect
                    if (SimpleCDNHandler.this.testSupplyabilityOrRedirect(requestedFile, exchange)) {
                        try {
                            // recicle old working code
                            HttpSchedulableReadCommand.handle(SimpleCDNHandler.this.executor, exchange, SimpleCDNHandler.this.identity);
                        } catch (Exception e) { }
                    }                
                    break;
                default:
                    methodNotAllowed(exchange);
                    break;
            }
        }

    }

    private void startClientEndpoints() {

    }

    private void stopClientEndpoints() {

    }
    // Operations associated with client endpoints --------------------
}
