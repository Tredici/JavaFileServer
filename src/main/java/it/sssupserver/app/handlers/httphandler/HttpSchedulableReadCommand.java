package it.sssupserver.app.handlers.httphandler;

import it.sssupserver.app.commands.*;
import it.sssupserver.app.commands.schedulables.*;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.users.Identity;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;

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

    private class Result extends SchedulableSizeCommand {
        @Override
        public void setUser(Identity user) {}

        @Override
        public Identity getUser() {
            return HttpSchedulableReadCommand.this.user;
        }

        private Semaphore sem;
        private boolean status;
        long size = 0;
        public Result(SizeCommand cmd) {
            super(cmd);
            this.sem = new Semaphore(0);
        }

        public void notFound() {
            status = false;
            this.sem.release();
        }

        @Override
        public void reply(long size) {
            status = true;
            this.size = size;
            this.sem.release();
        }

        public boolean success() throws InterruptedException {
            this.sem.acquire();
            return status;
        }

        public long getSize() {
            return this.size;
        }
    }

    private Result myResult;

    private HttpSchedulableReadCommand(ReadCommand cmd, HttpExchange exchange) {
        super(cmd);
        this.myResult = new Result(new SizeCommand(getPath()));
        this.exchange = exchange;
    }
    
    // wait untill the size is get from
    private boolean waitForSizeOrFail() throws Exception {
        var success = this.myResult.success();
        return success;
    }
    
    private Result getSizeQuery() {
        return this.myResult;
    }

    // send header with size
    private void startReadBodyResponse() throws IOException {
        var resHead = exchange.getResponseHeaders();
        resHead.add("Content-Disposition", "attachment");
        exchange.sendResponseHeaders(200, this.myResult.getSize());
    }

    // if not found
    @Override
    public void notFound() throws Exception {
        exchange.sendResponseHeaders(404, 0);
        exchange.close();
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

    public static void handle(FileManager executor, HttpExchange exchange) throws Exception {
        handle(executor, exchange, null);
    }

    public static void handle(FileManager executor, HttpExchange exchange, Identity user) throws Exception {
        var uri = exchange.getRequestURI();
        var path = uri.getPath();
        var fixedPath = HttpHelper.normalizePath(path);
        var cmd = new ReadCommand(fixedPath);
        var schedulable = new HttpSchedulableReadCommand(cmd, exchange);
        if (user != null) {
            schedulable.setUser(user);
        }
        executor.scheduleExecution(schedulable.getSizeQuery());
        // block to get file size
        if (schedulable.waitForSizeOrFail()) {
            schedulable.startReadBodyResponse();
            executor.scheduleExecution(schedulable);
        } else {
            // send 404
            schedulable.notFound();
        }
    }
}
