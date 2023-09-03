package it.sssupserver.app;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import it.sssupserver.app.base.HexUtils;

public class HexTest {
    
    @Test
    public void hexToBytes() {
        var h = Integer.toHexString(-1);
        var b = HexUtils.hexToBytes(h);
        assertTrue("Bad lenght", b.length == 4);
    }

    @Test(expected = IllegalArgumentException.class)
    public void badLength() {
        HexUtils.hexToBytes("1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void badContent() {
        HexUtils.hexToBytes("zz");
    }
}
