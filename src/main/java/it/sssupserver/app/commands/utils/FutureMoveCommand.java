package it.sssupserver.app.commands.utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import it.sssupserver.app.base.Path;
import it.sssupserver.app.commands.MoveCommand;
import it.sssupserver.app.commands.schedulables.SchedulableMoveCommand;
import it.sssupserver.app.filemanagers.FileManager;
import it.sssupserver.app.users.Identity;

public class FutureMoveCommand extends SchedulableMoveCommand {
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
                "Failed to move '" + getSource().toString() + "' to '"
                + getDestination().toString() + "'"));
        }
    }

    private CompletableFuture<Boolean> future;
    private FutureMoveCommand(MoveCommand cmd) {
        super(cmd);
        future = new CompletableFuture<>();
    }


    public static Future<Boolean> move(FileManager executor, Path src, Path dst, Identity user) throws Exception {
        var cmd = new MoveCommand(src, dst);
        var schedulable = new FutureMoveCommand(cmd);
        if (user != null) {
            schedulable.setUser(user);
        }
        executor.scheduleExecution(schedulable);
        return schedulable.future;
    }
}
