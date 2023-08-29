package it.sssupserver.app.commands.utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import it.sssupserver.app.base.InvalidPathException;
import it.sssupserver.app.base.Path;
import it.sssupserver.app.commands.SizeCommand;
import it.sssupserver.app.commands.schedulables.SchedulableSizeCommand;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.users.Identity;

public class FutureFileSizeCommand extends SchedulableSizeCommand {
    private Identity user;
    @Override
    public void setUser(Identity user) {
        this.user = user;
    }

    @Override
    public Identity getUser() {
        return this.user;
    }

    private CompletableFuture<Long> future;

    private FutureFileSizeCommand(SizeCommand cmd) {
        super(cmd);
        future = new CompletableFuture<Long>();
    }

    @Override
    public void reply(long size) throws Exception {
        future.complete(size);
    }

    @Override
    public void notFound() throws Exception {
        future.completeExceptionally(new RuntimeException("File not found: " + getPath().toString()));
    }

    public static Future<Long> querySize(FileManager executor, String path, Identity user) throws InvalidPathException, Exception {
        return querySize(executor, new Path(path, false), user);
    }

    public static Future<Long> querySize(FileManager executor, Path path, Identity user) throws Exception {
        if (path.isDir()) {
            throw new IllegalArgumentException("path must refer to a file");
        }
        var cmd = new FutureFileSizeCommand(new SizeCommand(path));
        if (user != null) {
            cmd.setUser(user);
        }
        executor.scheduleExecution(cmd);
        return cmd.future;
    }
}
