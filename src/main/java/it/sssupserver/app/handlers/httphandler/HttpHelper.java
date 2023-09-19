package it.sssupserver.app.handlers.httphandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.tika.Tika;

import it.sssupserver.app.base.InvalidPathException;
import it.sssupserver.app.base.Path;

import com.sun.net.httpserver.HttpExchange;

public class HttpHelper {

    private static Tika mimeDetector = new Tika();
    // Estimate MIME Type from file name
    public static String estimateMimeType(String filename) {
        String ans = mimeDetector.detect(filename);
        return ans != null ? ans : "application/octet-stream";
    }

    public static String escapeString(String str) {
        return str.replaceAll("\\\\", "\\")
            .replaceAll("\n", "\\n")
            .replaceAll("\t", "\\t")
            .replaceAll("\"", "\\\"");
    }

    public static Path normalizePath(String path) throws InvalidPathException {
        Stack<String> pathStack = new Stack<>();
        boolean isDir = false;
        for (var el : path.split("/")) {
            switch (el) {
                case "": // do nothing
                    isDir = true;
                    break;
                    case ".": // do nothing
                    isDir = true;
                    break;
                case "..": // pop position
                    isDir = true;
                    if (!pathStack.empty()) {
                        pathStack.pop();
                    }
                    break;
                default:
                    isDir = false;
                    pathStack.add(el);
            }
        }
        var fixedPath = String.join("/", pathStack.toArray(new String[0]));

        return new Path(fixedPath, isDir);
    }


    public static Map<String, String> extractPostParametersOr400(HttpExchange exchange, String[] required) {
        Map<String, String> ans = new HashMap<>();
        // TODO - check encoding
        var httpheader = exchange.getRequestHeaders();
        if (!httpheader.containsKey("Content-Type")) {
            var os = exchange.getResponseBody();
            try {
                os.flush();
                os.close();
            } catch (Exception e) { }
            return null;
        }
        var list = httpheader.get("Content-Type");
        if (list.size() != 1) {
            // Bad Request
        }
        var ContentType = httpheader.getFirst("Content-Type"); // list[0];
        switch (ContentType) {
            case "application/x-www-form-urlencoded":
                break;
            default:
                break;
        }
        //Content-Type: application/x-www-form-urlencoded
        // TODO - extract parameters from body

        // assert existence of required parameters
        var parameters = ans.keySet();
        for (var r : required) {
            if (!parameters.contains(r)) {
                // 400!
            }
        }

        return ans;
    }
}
