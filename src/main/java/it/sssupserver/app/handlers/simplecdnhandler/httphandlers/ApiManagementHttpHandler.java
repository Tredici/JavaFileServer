package it.sssupserver.app.handlers.simplecdnhandler.httphandlers;

import java.io.IOException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import it.sssupserver.app.handlers.simplecdnhandler.DataNodeDescriptor;
import it.sssupserver.app.handlers.simplecdnhandler.SearchPathWithHashGson;
import it.sssupserver.app.handlers.simplecdnhandler.SimpleCDNHandler;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import static it.sssupserver.app.handlers.httphandler.HttpResponseHelpers.*;
import static it.sssupserver.app.handlers.simplecdnhandler.FilenameCheckers.*;

// listen for /api calls
public class ApiManagementHttpHandler implements HttpHandler {
    public static final String PATH = "/api";

    private SimpleCDNHandler handler;
    public ApiManagementHttpHandler(SimpleCDNHandler handler) {
        this.handler = handler;
    }

    private void sendTopologyWithIdentity(HttpExchange exchange) throws IOException {
        var topo = handler.topologyWithMeToJson(false);
        sendJson(exchange, topo);
    }

    private void sendIdentity(HttpExchange exchange) throws IOException {
        var me = handler.thisnodeAsJson();
        sendJson(exchange, me);
    }

    private void sendLocalStorageInfoWithIdentity(HttpExchange exchange) throws IOException {
        var info = handler.regularFilesInfoWithMeToJson(false);
        sendJson(exchange, info);
    }

    private void sendSuppliableFileInfoWithIdentity(HttpExchange exchange) throws IOException {
        var info = handler.suppliableFilesWithIdentity(false, false);
        sendJson(exchange, info);
    }

    private void sendSuppliableFileInfoDetailedWithIdentity(HttpExchange exchange) throws IOException {
        var info = handler.suppliableFilesWithIdentity(false, true);
        sendJson(exchange, info);
    }

    private void sendFileStatisticsWithIdentity(HttpExchange exchange) throws IOException {
        var stats = handler.statisticsWithIdentity(false);
        sendJson(exchange, stats);
    }

    private void sendConfiguration(HttpExchange exchange) throws IOException {
        var conf = handler.getConfig().toJson();
        sendJson(exchange, conf);
    }

    private void sendFileHash(HttpExchange exchange, String searchPath) throws IOException {
        if (!isValidPathName(searchPath)) {
            httpBadRequest(exchange, "Invalid path: " + searchPath);
        } else {
            var hash = handler.hashFilePath(searchPath);
            var h = SearchPathWithHashGson.toJson(searchPath, hash);
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
            handler.watchIfNotWatched(other);
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
            handler.watchIfNotWatched(other);
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

