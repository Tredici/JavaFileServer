package it.sssupserver.app.commands.utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import it.sssupserver.app.base.Path;
import it.sssupserver.app.commands.DeleteCommand;
import it.sssupserver.app.commands.schedulables.SchedulableDeleteCommand;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.users.Identity;

public class FutureDeleteCommand extends SchedulableDeleteCommand {
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
    private FutureDeleteCommand(Path path) {
        super(new DeleteCommand(path));
        future = new CompletableFuture<>();
    }

    public static Future<Boolean> delete(FileManager executor, Path path, Identity user) throws Exception {
        var cmd = new FutureDeleteCommand(path);
        if (user != null) {
            cmd.setUser(user);
        }
        executor.scheduleExecution(cmd);
        return cmd.future;
    }
}
