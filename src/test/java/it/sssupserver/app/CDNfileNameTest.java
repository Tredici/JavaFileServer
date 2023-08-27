package it.sssupserver.app;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import it.sssupserver.app.handlers.simplecdnhandler.SimpleCDNHandler;

public class CDNfileNameTest {

    @Test
    public void preventEmptyName() {
        //assertFalse(SimpleCDNHandler.isValidPathName("/"));
        assertFalse(SimpleCDNHandler.isValidPathName(""));
    }

    @Test
    public void goodNames() {
        assertTrue(SimpleCDNHandler.isValidPathName("a/b/c.html"));
        assertTrue(SimpleCDNHandler.isValidPathName("a.html"));
    }

    @Test
    public void badNames() {
        assertFalse(SimpleCDNHandler.isValidPathName("a/b/.html"));
        assertFalse(SimpleCDNHandler.isValidPathName("a/b/.html"));
        assertFalse(SimpleCDNHandler.isValidPathName("a/b/c..html"));
        assertFalse(SimpleCDNHandler.isValidPathName("a/b/c."));
        assertFalse(SimpleCDNHandler.isValidPathName("."));
    }
}
