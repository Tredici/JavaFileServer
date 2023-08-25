package it.sssupserver.app.handlers.httphandler;

import it.sssupserver.app.commands.DeleteCommand;
import it.sssupserver.app.commands.schedulables.SchedulableDeleteCommand;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.users.Identity;

import com.sun.net.httpserver.HttpExchange;

public class HttpSchedulableDeleteCommand extends SchedulableDeleteCommand  {
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
    public HttpSchedulableDeleteCommand(DeleteCommand cmd, HttpExchange exchange) {
        super(cmd);
        this.exchange = exchange;
    }

    @Override
    public void reply(boolean success) throws Exception {
        if (success) {
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().flush();
            exchange.close();    
        } else {
            exchange.sendResponseHeaders(404, 0);
            exchange.getResponseBody().flush();
            exchange.close();
        }
    }

    public static void handle(FileManager executor, HttpExchange exchange) throws Exception {
        var uri = exchange.getRequestURI();
        var path = uri.getPath();
        var fixedPath = HttpHelper.normalizePath(path);
        var cmd = new DeleteCommand(fixedPath);
        var schedulable = new HttpSchedulableDeleteCommand(cmd, exchange);
        executor.scheduleExecution(schedulable);
    }
}
