package it.sssupserver.app.commands.utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import it.sssupserver.app.base.Path;
import it.sssupserver.app.commands.MkdirCommand;
import it.sssupserver.app.commands.schedulables.SchedulableMkdirCommand;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.users.Identity;

/**
 * Used to create directories when required by other code
 */
public class FutureMkdirCommand extends SchedulableMkdirCommand {
    private FutureMkdirCommand(MkdirCommand cmd) {
        super(cmd);
        future = new CompletableFuture<>();
    }

    private Identity user;
    @Override
    public void setUser(Identity user) {
        this.user = user;
    }

    @Override
    public Identity getUser() {
        return this.user;
    }

    @Override
    public void reply(boolean success) throws Exception {
        if (success) {
            future.complete(success);
        } else {
            future.completeExceptionally(new RuntimeException(
                "Failed to delete '" + getPath().toString() + "'"));
        }
    }
    
    private CompletableFuture<Boolean> future;
    public static Future<Boolean> create(FileManager executor, Path path, Identity user) throws Exception {
        var cmd = new FutureMkdirCommand(new MkdirCommand(path));
        if (user != null) {
            cmd.setUser(user);
        }
        cmd.setRecursive(true);
        executor.scheduleExecution(cmd);
        return cmd.future;
    }
}
