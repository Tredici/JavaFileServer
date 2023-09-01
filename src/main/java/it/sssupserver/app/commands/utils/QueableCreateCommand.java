package it.sssupserver.app.commands.utils;

import java.util.concurrent.CompletableFuture;

import it.sssupserver.app.base.BufferManager;
import it.sssupserver.app.base.Path;
import it.sssupserver.app.commands.schedulables.SchedulableCreateCommand;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.users.Identity;

/**
 * Similar to QueableAppendCommand
 */
public class QueableCreateCommand extends SchedulableCreateCommand implements QueableCommand {
    private Identity user;
    @Override
    public void setUser(Identity user) {
        this.user = user;
    }

    @Override
    public Identity getUser() {
        return this.user;
    }

    private CompletableFuture<Boolean> future;
    @Override
    public void reply(boolean success) throws Exception {
        // free resources
        wrapper.close();
        // mark result as completed
        future.complete(success);
    }

    private QueableCreateCommand(Path path, BufferManager.BufferWrapper data) {
        super(path, data.get(), false);
        wrapper = data;
        future = new CompletableFuture<>();
    }

    @Override
    public QueableAppendCommand enqueue(BufferManager.BufferWrapper data) {
        // optimistic: create object out of synchronized
        var obj = new QueableAppendCommand(getPath(), data);
        // same executor
        obj.executor = executor;
        // same user
        obj.setUser(getUser());
        future.thenAccept(result -> {
            try {
                if (result) {
                    // if success enqueue
                    executor.scheduleExecution(obj);
                } else {
                    // if fail cancel recursively
                    obj.reply(false);
                }
            } catch (Exception e) {
                // free resources
                obj.wrapper.close();
            }
        });
        // allow chaining
        return obj;
    }

    @Override
    public CompletableFuture<Boolean> getFuture() {
        return future;
    }

    private FileManager executor;
    private BufferManager.BufferWrapper wrapper;

    public static QueableCreateCommand submit(FileManager executor, Path path, Identity user, BufferManager.BufferWrapper data) throws Exception {
        // can only create regular files
        if (path.isDir()) {
            throw new IllegalArgumentException("path must refer to a regular file");
        }
        var cmd = new QueableCreateCommand(path, data);
        cmd.executor = executor;
        if (user != null) {
            cmd.setUser(user);
        }
        executor.scheduleExecution(cmd);
        return cmd;
    }
}
