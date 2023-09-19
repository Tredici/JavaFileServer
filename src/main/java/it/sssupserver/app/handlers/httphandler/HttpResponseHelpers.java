package it.sssupserver.app.handlers.httphandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpExchange;

public class HttpResponseHelpers {

    public static void sendJson(HttpExchange exchange, String json) throws IOException {
        var bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        var os = exchange.getResponseBody();
        os.write(bytes);
        exchange.close();
    }

    public static void httpOk(HttpExchange exchange, String error) {
        try {
            // 200 OK
            //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/200
            exchange.sendResponseHeaders(200, 0);
            if (error != null && !error.isBlank()) {
                var os = exchange.getResponseBody();
                os.write(error.getBytes(StandardCharsets.UTF_8));
                os.flush();;
            } else {
                exchange.getResponseBody().flush();
            }
            exchange.close();
        } catch (Exception e) { System.err.println(e); e.printStackTrace(); }
    }

    /**
     * 201 Created
     *  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/201
     */
    public static void httpCreated(HttpExchange exchange, String error) {
        try {
            exchange.sendResponseHeaders(201, 0);
            if (error != null && !error.isBlank()) {
                var os = exchange.getResponseBody();
                os.write(error.getBytes(StandardCharsets.UTF_8));
                os.flush();;
            } else {
                exchange.getResponseBody().flush();
            }
            exchange.close();
        } catch (Exception e) { System.err.println(e); e.printStackTrace(); }
    }

    /**
     * 304 Not Modified
     *  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/304
     */
    public static void httpNotModified(HttpExchange exchange) {
        try {
            exchange.sendResponseHeaders(304, 0);
            exchange.getResponseBody().flush();
            exchange.close();
        } catch (Exception e) { System.err.println(e); e.printStackTrace(); }
    }

    public static void httpBadRequest(HttpExchange exchange) {
        httpBadRequest(exchange, null);
    }

    public static void httpBadRequest(HttpExchange exchange, String error) {
        try {
            // 400 Bad request
            //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/400
            exchange.sendResponseHeaders(400, 0);
            if (error != null && !error.isBlank()) {
                var os = exchange.getResponseBody();
                os.write(error.getBytes(StandardCharsets.UTF_8));
                os.flush();;
            } else {
                exchange.getResponseBody().flush();
            }
            exchange.close();
        } catch (Exception e) { System.err.println(e); e.printStackTrace(); }
    }

    public static void httpMethodNotAllowed(HttpExchange exchange) {
        try {
            var res = new StringBuilder()
                .append("405 Method Not Allowed")
                .append("\n")
                .toString()
                .getBytes(StandardCharsets.UTF_8);
            // 405 Method Not Allowed
            //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/405
            exchange.sendResponseHeaders(405, res.length);
            exchange.getResponseBody().write(res);
            exchange.getResponseBody().flush();
            exchange.close();
        } catch (Exception e) { System.err.println(e); e.printStackTrace(); }
    }

    public static void httpNotFound(HttpExchange exchange) {
        httpNotFound(exchange, "404 Not Found");
    }

    public static void httpGone(HttpExchange exchange) {
        try {
            // 410 Gone
            //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/410
            exchange.sendResponseHeaders(410, 0);
            exchange.getResponseBody().flush();
            exchange.close();
        } catch (Exception e) { System.err.println(e); e.printStackTrace(); }
    }

    public static void httpNotFound(HttpExchange exchange, String error) {
        try {
            var msg = error != null ? error.getBytes(StandardCharsets.UTF_8) : null;
            exchange.sendResponseHeaders(404, msg != null ? msg.length : 0);
            if (error != null && !error.isBlank()) {
                var os = exchange.getResponseBody();
                os.write(msg);
                os.flush();
            } else {
                exchange.getResponseBody().flush();
            }
            exchange.close();
        } catch (Exception e) { System.err.println(e); e.printStackTrace(); }
    }

    public static void httpInternalServerError(HttpExchange exchange) {
        try {
            exchange.sendResponseHeaders(500, 0);
            exchange.getResponseBody().flush();
            exchange.close();
        } catch (Exception e) { System.err.println(e); e.printStackTrace(); }
    }

}
