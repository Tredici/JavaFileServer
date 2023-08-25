package it.sssupserver.app;

import java.nio.file.Paths;

import org.junit.Test;

import it.sssupserver.app.commands.utils.ListTreeCommand;
import it.sssupserver.app.filemanagers.samples.UserTreeFileManager;

public class FileManagerTest {
    

    @Test
    public void testListTreeCommand() throws Exception {
        var cwd = Paths.get("").resolve("server_dir");
        var executor = new UserTreeFileManager(cwd);

        ListTreeCommand.explore(executor, "", null);
    }
}
