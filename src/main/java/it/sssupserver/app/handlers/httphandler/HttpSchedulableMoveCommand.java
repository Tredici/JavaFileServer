package it.sssupserver.app.handlers.httphandler;

import it.sssupserver.app.base.BufferManager;
import it.sssupserver.app.base.Path;
import it.sssupserver.app.commands.*;
import it.sssupserver.app.commands.schedulables.*;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.users.Identity;

import java.nio.channels.SocketChannel;

import com.sun.net.httpserver.HttpExchange;

public class HttpSchedulableMoveCommand extends SchedulableMoveCommand {
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
    protected HttpSchedulableMoveCommand(MoveCommand cmd, HttpExchange exchange) {
        super(cmd);
        this.exchange = exchange;
    }

    @Override
    public void reply(boolean success) throws Exception {
        if (success) {
            exchange.sendResponseHeaders(200, 0);
        } else {
            exchange.sendResponseHeaders(500, 0);
        }
        var os = exchange.getResponseBody();
        os.flush();
        os.close();
    }

    public static void handle(FileManager executor, HttpExchange exchange) throws Exception {
        var required = new String[]{ "src", "dst" };
        var pars = HttpHelper.extractPostParametersOr400(exchange, required);
        var src = pars.get("src");
        var dst = pars.get("dst");
        var cmd = new MoveCommand(new Path(src), new Path(dst));
        var schedulable = new HttpSchedulableModeCommand(cmd, exchange);
        executor.scheduleExecution(schedulable);
    }
}
