package it.sssupserver.app;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Test;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import it.sssupserver.app.handlers.simplecdnhandler.SimpleCDNConfiguration;

public class JsonConfigTest {
    
    /**
     * Rigorous Test :-)
     * @throws JsonIOException
     * @throws JsonSyntaxException
     * @throws IOException
     */
    @Test
    public void shouldNotThrow() throws JsonSyntaxException, JsonIOException, IOException
    {
        var cwd = Paths.get("").toAbsolutePath();
        var path = cwd.resolve("SimpleCDN.json").toString();

        System.out.println("CWD:  " + cwd);
        System.out.println("File: " + path);

        assertTrue("Configuration file not found", Files.exists(new File(path).toPath()));

        try (var fr = new FileReader(path)) {
            CharBuffer buf = CharBuffer.allocate(4096);
            fr.read(buf);
            buf.flip();
            System.out.println("Config content:\n" + buf.toString());
        }

        var config = SimpleCDNConfiguration.parseJsonCondifuration(path);
        System.out.println("var config != null: " + (config != null));
        assertTrue("Error in config: " + path, config != null);
        var gson = new GsonBuilder().setPrettyPrinting().create();
        var jconf = gson.toJson(config);
        System.out.println("Parsed config: " + jconf);
    }    
}
