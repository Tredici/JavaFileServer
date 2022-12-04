package it.sssupserver.app.handlers.httphandler;

import it.sssupserver.app.commands.SizeCommand;
import it.sssupserver.app.commands.schedulables.SchedulableSizeCommand;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.users.Identity;

import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpExchange;

public class HttpSchedulableSizeCommand extends SchedulableSizeCommand {
    private Identity user;
    @Override
    public void setUser(Identity user) {
        this.user = user;
    }

    @Override
    public Identity getUser() {
        return this.user;
    }
    
    private HttpExchange exchange;
    public HttpSchedulableSizeCommand(SizeCommand cmd, HttpExchange exchange) {
        super(cmd);
        this.exchange = exchange;
    }

    @Override
    public void notFound() throws Exception {
        exchange.sendResponseHeaders(404, 0);
        exchange.getResponseBody().flush();
        exchange.close();
    }

    public static void handle(FileManager executor, HttpExchange exchange) throws Exception {
        var uri = exchange.getRequestURI();
        var path = uri.getPath();
        var fixedPath = HttpHelper.normalizePath(path);
        var cmd = new SizeCommand(fixedPath);
        var schedulable = new HttpSchedulableSizeCommand(cmd, exchange);
        executor.scheduleExecution(schedulable);
    }

    @Override
    public void reply(long size) throws Exception {
        var path = getPath().toString();
        var escapedPath = HttpHelper.escapeString(path);
        var ans =  new StringBuilder()
            .append("{\"file\":\"").append(escapedPath).append("\",")
            .append("\"size\":").append(Long.toString(size))
            .append("}").toString().getBytes(StandardCharsets.UTF_8);
        // https://www.w3.org/Protocols/HTTP/1.0/draft-ietf-http-spec.html#Content-Length
        exchange.sendResponseHeaders(200, ans.length);
        var os = exchange.getResponseBody();
        os.write(ans);
        os.flush();
        exchange.close();
    }
}
