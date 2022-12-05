package it.sssupserver.app.handlers.httphandler;

import java.io.IOException;
import it.sssupserver.app.filemanagers.FileManager;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class HttpRequestHandler implements HttpHandler {

    private FileManager executor;

    public HttpRequestHandler(FileManager executor) {
        this.executor = executor;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            switch (exchange.getRequestMethod()) {
                case "GET":
                    {
                        String req = "";
                        var query = exchange.getRequestURI().getQuery();
                        if (query != null) {
                            for (var v : query.split("&")) {
                                if (v.equals("size") || v.equals("size=")) {
                                    req = "size"; break;
                                } else
                                if (v.equals("list") || v.equals("list=")) {
                                    req = "list"; break;
                                }
                            }
                        }
                        switch (req) {
                        case "list":
                            HttpSchedulableListCommand.handle(this.executor, exchange);
                            break;
                        case "size":
                            HttpSchedulableSizeCommand.handle(this.executor, exchange);
                            break;
                        default:
                            HttpSchedulableReadCommand.handle(this.executor, exchange);
                        }
                    }
                    break;
                case "DELETE":
                    HttpSchedulableDeleteCommand.handle(this.executor, exchange);
                    break;
                default:
                    // Not Implemented
                    notImplemented(exchange);
            }
        } catch (Exception e) {
            if (exchange.getResponseCode() != -1) {
                // internal server error
                exchange.sendResponseHeaders(500, 0);
            }
            exchange.getResponseBody().flush();
            exchange.close();
        }
    }
    

    private static void notImplemented(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(501, 0);
        exchange.getResponseBody().flush();
        exchange.close();
    }
}
