package it.sssupserver.app.handlers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.stream.Collectors;

import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.handlers.httphandler.HttpHandler;
import it.sssupserver.app.handlers.simplebinaryhandler.*;
import it.sssupserver.app.handlers.simplecdnhandler.SimpleCDNHandler;

/**
 * This class will be used to supply request
 * hadlers based on the supplied parameters
 */
public class RequestHandlerFactory {
    // Command line arguments starting with this prefix
    // are intended as directed to a request handler
    private static final String argsPrefix = "--H";

    private static class HandlerArgs {
        public int port = 0;
        public String host;
        public String method = "SBP";
        public List<Map.Entry<String, String>> extras = new ArrayList<>();
    }

    public static void Help(String lpadding) {
        if (lpadding == null) {
            lpadding = "";
        }
        System.err.println(lpadding + "Arguments recognised by request handlers:");
        System.err.println(lpadding + "\t" + argsPrefix + "port number: port number on which listen for requests");
        System.err.println(lpadding + "\t" + argsPrefix + "host hostname/ip: hostname or ip on which listen for requests");
        System.err.println(lpadding + "\t" + argsPrefix + "http: use http request handler");
        System.err.println(lpadding + "\t" + argsPrefix + "cdn: use SimpleCDN request handler");
        System.err.println(lpadding + "\t" + argsPrefix + "extra: key1=val1[;key2=val2[;key3=val3[...]]] extra handler-specific KV args");
    }

    private static HandlerArgs parseArgs(String[] args) {
        var ans = new HandlerArgs();
        try {
            boolean port = false, host = false;
            if (args != null) for (int i=0; i!=args.length; ++i) {
                var a = args[i];
                if (a.equals("--")) {
                    break;
                } else if (a.startsWith(argsPrefix)) {
                    var parameter = a.substring(argsPrefix.length());
                    switch (parameter) {
                        case "port":
                            if (port) {
                                throw new Exception("Duplicated parameter port");
                            }
                            if (i + 1 == args.length) {
                                throw new Exception("Missing value for parameter port");
                            }
                            ans.port = Integer.parseInt(args[++i]);
                            port = true;
                            break;
                        case "host":
                            if (host) {
                                throw new Exception("Duplicated parameter host");
                            }
                            if (i + 1 == args.length) {
                                throw new Exception("Missing value for parameter host");
                            }
                            ans.host = args[++i];
                            host = true;
                            break;
                        case "http":
                            ans.method = "http";
                            break;
                        case "cdn":
                            ans.method = "cdn";
                            break;
                        case "extra":
                            {
                                if (i + 1 == args.length) {
                                    throw new Exception("Missing value for parameter host");
                                }
                                var values = args[++i];
                                var pairs = Arrays.stream(values.split(";"))
                                    .map(s -> s.split("="));
                                if (pairs.anyMatch(p -> p.length != 2)) {
                                    throw new Exception("Bad extra parameter: " + values);
                                }
                                pairs = Arrays.stream(values.split(";"))
                                    .map(s -> s.split("="));
                                ans.extras = pairs.map(p -> new
                                    SimpleImmutableEntry<String, String>(p[0], p[1]))
                                    .collect(Collectors.toList())
                                    //.toArray(SimpleImmutableEntry<String, String>[]::new)
                                    ;
                            }
                            break;
                        default:
                            throw new Exception("Unrecognised argument '" + a + "'");
                    }
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            Help("\t");
            System.exit(1);
        }
        return ans;
    }

    public static RequestHandler getRequestHandler(FileManager executor) throws Exception {
        return getRequestHandler(executor, null);
    }

    public static RequestHandler getRequestHandler(FileManager executor, String[] args) throws Exception {
        var a = parseArgs(args);
        RequestHandler ans;
        switch (a.method) {
            case "http":
                ans = new HttpHandler(executor, a.port, a.host);
                break;
            case "cdn":
                ans = new SimpleCDNHandler(executor, a.extras);
                break;
            default:
                ans = new SimpleBinaryHandler(executor, a.port, a.host);
                break;
        }
        return ans;
    }
}
