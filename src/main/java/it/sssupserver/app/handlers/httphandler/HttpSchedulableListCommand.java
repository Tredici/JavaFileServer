package it.sssupserver.app.handlers.httphandler;

import it.sssupserver.app.base.Path;
import it.sssupserver.app.commands.ListCommand;
import it.sssupserver.app.commands.schedulables.SchedulableListCommand;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.users.Identity;

import java.nio.charset.StandardCharsets;
import java.util.Collection;

import com.sun.net.httpserver.HttpExchange;

public class HttpSchedulableListCommand extends SchedulableListCommand {
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
    public HttpSchedulableListCommand(ListCommand cmd, HttpExchange exchange) {
        super(cmd);
        this.exchange = exchange;
    }

    @Override
    public void notFound() throws Exception {
        exchange.sendResponseHeaders(404, 0);
        exchange.getResponseBody().flush();
        exchange.close();
    }

    @Override
    public void reply(Collection<Path> content) throws Exception {
        var path = getPath().toString();
        var escapedPath = HttpHelper.escapeString(path);
        var strbuilder = new StringBuilder()
            .append("{\"location\":\"").append(escapedPath).append("\",")
            .append("\"files\":[");
        boolean first = true;
        for (var f : content) {
            if (!first) {
                strbuilder.append(",");
            } else {
                first = false;
            }
            escapedPath = HttpHelper.escapeString(f.toString());
            strbuilder.append("\"").append(escapedPath);
            if (f.isDir()) {
                strbuilder.append("/");
            }
            strbuilder.append("\"");
        }
        var ans = strbuilder.append("]}").toString().getBytes(StandardCharsets.UTF_8);
        // https://www.w3.org/Protocols/HTTP/1.0/draft-ietf-http-spec.html#Content-Length
        exchange.sendResponseHeaders(200, ans.length);
        var os = exchange.getResponseBody();
        os.write(ans);
        os.flush();
        exchange.close();        
    }

    public static void handle(FileManager executor, HttpExchange exchange) throws Exception {
        var uri = exchange.getRequestURI();
        var path = uri.getPath();
        var fixedPath = HttpHelper.normalizePath(path);
        var cmd = new ListCommand(fixedPath);
        var schedulable = new HttpSchedulableListCommand(cmd, exchange);
        executor.scheduleExecution(schedulable);
    }
}
