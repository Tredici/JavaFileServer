package it.sssupserver.app;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.Test;

import it.sssupserver.app.base.Path;
import it.sssupserver.app.commands.utils.FileReducerCommand;
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
        // print dirs
        var dirs = fsTree.getDirectories();
        for (var path : dirs) {
            System.out.println("\t" + path);
        }
        // print regural files
        var regFiles = fsTree.getRegularFiles();
        System.out.println("Regular files");
        for (var path : regFiles) {
            System.out.println("\t" + path);
        }
    }

    @Test
    public void testHash() throws Exception {
        var cwd = Paths.get("").toAbsolutePath().resolve("server_dir");
        var executor = new UserTreeFileManager(cwd);
        executor.start();
        var f = ListTreeCommand.explore(executor, "", null);
        var fsTree = f.get();
        // print regural files
        var regFiles = fsTree.getRegularFiles();
        // rely on parallelism
        var hashes = Arrays.stream(regFiles).map((Function<Path,Future<byte[]>>)p -> {
            try {
                return FileReducerCommand.reduceByHash(executor, p, null, FileReducerCommand.MD5);
            } catch (Exception e) {
                e.printStackTrace();
                var ff = new CompletableFuture<byte[]>();
                ff.completeExceptionally(e);
                return ff;
            }
        }).toArray();

        System.out.println("Regular files");
        for (var i=0; i<regFiles.length; ++i) {
            var path = regFiles[i];
            var hash = ((Future<byte[]>)hashes[i]).get();
            System.out.println("\t" + path);
            System.out.println("\thash:\t" +  bytesToHex(hash));
        }
        executor.stop();
    }

    // Code source:
    //  https://stackoverflow.com/questions/9655181/java-convert-a-byte-array-to-a-hex-string
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toLowerCase().toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }
}
