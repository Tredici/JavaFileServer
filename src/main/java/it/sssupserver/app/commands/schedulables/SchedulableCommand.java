package it.sssupserver.app.commands.schedulables;

import it.sssupserver.app.commands.Command;
import it.sssupserver.app.users.Identity;

/**
 * Represents a command with also the methods
 * to reply to the clients
 *
 * Maybe a method to return error could be useful
 */
public interface SchedulableCommand extends Command {
    public default Identity getUser() {
        return null;
    }
}
