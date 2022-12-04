package it.sssupserver.app.handlers.httphandler;

import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpServer;

import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.handlers.RequestHandler;


public class HttpHandler implements RequestHandler {

    private HttpServer httpserver;
    private HttpRequestHandler handler;

    private int port;
    private String ip;
    private FileManager executor;

    private static int DEFAULT_PORT = 8080;
    private static String DEFAULT_IP = "0.0.0.0";
    private static int BACKLOG = 30;

    private InetSocketAddress listening_address;

    public HttpHandler(FileManager executor) throws Exception
    {
        this(executor, DEFAULT_PORT, DEFAULT_IP);
    }

    public HttpHandler(FileManager executor, int port) throws Exception
    {
        this(executor, port, DEFAULT_IP);
    }

    public HttpHandler(FileManager executor, int port, String ip) throws Exception
    {
        if (!(port < (1<<16)))
        {
            throw new Exception("Invalid port number: " + port);
        }
        this.port = port != 0 ? port : DEFAULT_PORT;
        this.ip = ip != null ? ip : DEFAULT_IP;
        this.listening_address = new InetSocketAddress(this.ip, this.port);
        this.executor = executor;
        this.httpserver = HttpServer.create(this.listening_address, BACKLOG);
        this.handler = new HttpRequestHandler(this.executor);
        this.httpserver.createContext("/", this.handler);
    }

    @Override
    public void start() throws Exception {
        System.out.println("Listener will accept connections on address " + this.listening_address);
        httpserver.start();
    }

    @Override
    public void stop() throws Exception {
        System.out.println("Interrupt worker...");
        httpserver.stop(1);
        System.out.println("Worker terminated!");
    }
    
}
