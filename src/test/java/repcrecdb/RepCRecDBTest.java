/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package repcrecdb;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

class RepCRecDBTest {
    @Test void testInitialization() {
        TransactionManager tm = RepCRecDB.init();
        assertEquals(tm.dms.size(), 10);
    }

    @Test void testInstrRead() {
        TransactionManager tm = RepCRecDB.init();
        String instructions = "begin(T1)\nbegin(T2)\n";
        InputStream is = new ByteArrayInputStream(instructions.getBytes(StandardCharsets.UTF_8));
        tm.run(is);
        assertEquals(tm.transactions.size(), 2);
        Transaction t1 = tm.transactions.get("T1");
        assertNotNull(t1);
        assertEquals(t1.name, "T1");
        assertFalse(t1.isReadOnly);
        Transaction t2 = tm.transactions.get("T2");
        assertNotNull(t2);
        assertEquals(t2.name, "T2");
        assertFalse(t2.isReadOnly);
        assertTrue(t1.beginTime < t2.beginTime);
    }
}
