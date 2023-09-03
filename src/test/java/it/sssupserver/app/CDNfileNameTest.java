package it.sssupserver.app;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import it.sssupserver.app.handlers.simplecdnhandler.SimpleCDNHandler;

import static it.sssupserver.app.handlers.simplecdnhandler.FilenameCheckers.*;

public class CDNfileNameTest {

    @Test
    public void preventEmptyName() {
        //assertFalse(SimpleCDNHandler.isValidPathName("/"));
        assertFalse(isValidPathName(""));
    }

    @Test
    public void goodNames() {
        assertTrue(isValidPathName("a/b/c.html"));
        assertTrue(isValidPathName("a.html"));
    }

    @Test
    public void badNames() {
        assertFalse(isValidPathName("a/b/.html"));
        assertFalse(isValidPathName("a/b/.html"));
        assertFalse(isValidPathName("a/b/c..html"));
        assertFalse(isValidPathName("a/b/c."));
        assertFalse(isValidPathName("."));
    }
}
