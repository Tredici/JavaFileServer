package it.sssupserver.app.handlers.httphandler;

import it.sssupserver.app.base.Path;
import it.sssupserver.app.commands.*;
import it.sssupserver.app.commands.schedulables.*;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.users.Identity;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;


public class HttpSchedulableReadCommand extends SchedulableReadCommand {
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

    private class InternalSizeCommend extends SchedulableSizeCommand {
        @Override
        public void setUser(Identity user) {}

        @Override
        public Identity getUser() {
            return HttpSchedulableReadCommand.this.user;
        }

        long size = 0;
        public InternalSizeCommend(SizeCommand cmd) {
            super(cmd);
        }

        public void notFound() throws Exception {
            HttpSchedulableReadCommand.this.notFound();
        }

        @Override
        public void reply(long size) throws Exception {
            this.size = size;
            // enqueue read command
            // but send header
            HttpSchedulableReadCommand.this.startReadBodyResponse();
            executor.scheduleExecution(HttpSchedulableReadCommand.this);
        }

        public long getSize() {
            return this.size;
        }
    }

    private InternalSizeCommend myResult;

    private HttpSchedulableReadCommand(ReadCommand cmd, HttpExchange exchange) {
        super(cmd);
        this.myResult = new InternalSizeCommend(new SizeCommand(getPath()));
        this.exchange = exchange;
    }

    private InternalSizeCommend getSizeQuery() {
        return this.myResult;
    }

    // In case of good response (200 OK) insert these headers inside the
    // response
    private Map<String, String> onSuccessHeaders;
    private void setOnSuccessHeaders(Map<String, String> onSuccessHeaders) {
        this.onSuccessHeaders = onSuccessHeaders;
    }
    // send header with size
    private void startReadBodyResponse() throws IOException {
        var resHead = exchange.getResponseHeaders();
        if (onSuccessHeaders != null) {
            for (var pair : onSuccessHeaders.entrySet()) {
                resHead.add(pair.getKey(), pair.getValue());
            }
        }
        exchange.sendResponseHeaders(200, this.myResult.getSize());
    }

    // if not found
    @Override
    public void notFound() throws Exception {
        try {
            exchange.sendResponseHeaders(404, 0);
        } catch (Exception e) {
            exchange.close();
            throw e;
        }
    }

    @Override
    public void partial(ByteBuffer[] data) throws Exception {
        var os = exchange.getResponseBody();
        // send all file chunks
        byte[] bb = null;
        for (var buf : data) {
            if (!buf.isDirect()) {
                // array as backend
                os.write(buf.array());
            } else {
                // should use a byte[] as buffer
                var sz = buf.limit()-buf.position();
                if (bb == null || bb.length < sz) {
                    bb = new byte[sz];
                }
                buf.get(bb);
                os.write(bb);
            }
        }
    }

    @Override
    public void reply(ByteBuffer[] data) throws Exception {
        partial(data);
        exchange.getResponseBody().flush();
        // last chunk
        exchange.close();
    }

    /**
     * Reference to the file manager used to enqueue the
     */
    private FileManager executor;

    public static void handle(FileManager executor, HttpExchange exchange) throws Exception {
        handle(executor, exchange, null);
    }

    public static void handle(FileManager executor, HttpExchange exchange, Identity user, Path path, Map<String, String> onSuccessHeaders) throws Exception {
        var cmd = new ReadCommand(path);
        var schedulable = new HttpSchedulableReadCommand(cmd, exchange);
        schedulable.executor = executor;
        if (user != null) {
            schedulable.setUser(user);
        }
        if (onSuccessHeaders != null) {
            schedulable.setOnSuccessHeaders(onSuccessHeaders);
        }
        // enqueue size request, read command is possibly enqueued after
        executor.scheduleExecution(schedulable.getSizeQuery());
    }

    public static void handle(FileManager executor, HttpExchange exchange, Identity user, Path path) throws Exception {
        handle(executor, exchange, user, path, null);
    }

    public static void handle(FileManager executor, HttpExchange exchange, Identity user, Map<String, String> onSuccessHeaders) throws Exception {
        var uri = exchange.getRequestURI();
        var path = uri.getPath();
        var fixedPath = HttpHelper.normalizePath(path);
        handle(executor, exchange, user, fixedPath, onSuccessHeaders);
    }

    public static void handle(FileManager executor, HttpExchange exchange, Identity user) throws Exception {
        handle(executor, exchange, user, (Map<String, String>)null);
    }
}
