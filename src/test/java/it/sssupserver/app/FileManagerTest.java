package it.sssupserver.app;

import java.nio.file.Paths;

import org.junit.Test;

import it.sssupserver.app.commands.utils.ListTreeCommand;
import it.sssupserver.app.filemanagers.samples.UserTreeFileManager;

public class FileManagerTest {
    

    @Test
    public void testListTreeCommand() throws Exception {
        var cwd = Paths.get("").toAbsolutePath().resolve("server_dir");
        var executor = new UserTreeFileManager(cwd);
        executor.start();
        var f = ListTreeCommand.explore(executor, "", null);
        var fsTree = f.get();
        executor.stop();
        fsTree.print();
        System.out.println("As JSON:\n" + fsTree.toJson(true));
    }
}
