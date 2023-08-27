package it.sssupserver.app.commands.schedulables;

import it.sssupserver.app.commands.MkdirCommand;
import it.sssupserver.app.exceptions.ApplicationException;
import it.sssupserver.app.filemanagers.FileManager;

public abstract class SchedulableMkdirCommand extends MkdirCommand implements SchedulableCommand {

    // Should create also parent directories if they do not exist?
    private boolean recursive;
    public boolean isRecursive() {
        return recursive;
    }

    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
    }

    protected SchedulableMkdirCommand(MkdirCommand cmd)
    {
        super(cmd);
    }

    public abstract void reply(boolean success) throws Exception;

    @Override
    public final void submit(FileManager exe) throws ApplicationException {
        exe.handle(this);
    }
}
