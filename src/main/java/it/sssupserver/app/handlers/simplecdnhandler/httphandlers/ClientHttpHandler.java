package it.sssupserver.app.handlers.simplecdnhandler;

import java.io.IOException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import it.sssupserver.app.handlers.httphandler.HttpSchedulableReadCommand;
import java.net.URI;
import java.util.Collections;

import static it.sssupserver.app.handlers.httphandler.HttpResponseHelpers.*;


public class ClientHttpHandler implements HttpHandler {
    public static final String PATH = "/";

    private SimpleCDNHandler handler;
    public ClientHttpHandler(SimpleCDNHandler handler) {
        this.handler = handler;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            switch (exchange.getRequestMethod()) {
                case "GET":
                    var requestedFile = new URI(PATH).relativize(exchange.getRequestURI()).getPath();
                    // if end with /, gently use /index.html
                    if (requestedFile.isEmpty() || requestedFile.endsWith("/")) {
                        requestedFile += "index.html";
                    }
                    // check if current node is file supplier or redirect
                    if (handler.testSupplyabilityOrRedirect(requestedFile, exchange)) {
                        // prevent clients from directly accessing local files
                        var truePath = handler.getFsStatus().findTruePath(requestedFile);
                        if (truePath == null) {
                            // return 404
                            httpNotFound(exchange);
                            handler.incrementNotFoundCount(requestedFile);
                        } else {
                            try {
                                // Try to detect MIME type
                                var mime = handler.estimateMimeType(requestedFile);
                                var headers = Collections.singletonMap("Content-Type", mime);
                                // recicle old working code
                                HttpSchedulableReadCommand.handle(handler.getFileManager(),
                                    exchange,
                                    handler.getIdentity(),
                                    SimpleCDNHandler.sanitazePath(truePath),
                                    headers);
                                handler.incrementDownloadCount(requestedFile);
                            } catch (Exception e) { System.err.println(e); e.printStackTrace(); }
                        }
                    } else {
                        // one more redirect
                        handler.incrementRedirectCount(requestedFile);
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
