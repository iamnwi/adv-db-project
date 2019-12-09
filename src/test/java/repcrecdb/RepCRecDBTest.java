/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package repcrecdb;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

class RepCRecDBTest {
    @Test void testInitialization() {
        TransactionManager tm = RepCRecDB.init();
        assertEquals(tm.dms.size(), 10);
        for (DataManager dm : tm.dms.values()) {
            SiteStatus siteStatus = tm.siteStatusTable.get(dm.siteID);
            assertEquals(siteStatus.status, RunningStatus.UP);
            assertEquals(siteStatus.lastDownTime, tm.ticks);
        }
    }

    @Test void testInstrBegin() {
        TransactionManager tm = RepCRecDB.init();
        String instructions = "begin(T1)\nbegin(T2)\n";
        tm.run(stringToInputStream(instructions));
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

    @Test void testInstrBeginRO() {
        TransactionManager tm = RepCRecDB.init();
        String instructions = "beginRO(T1)\nbeginRO(T2)\n";
        for (DataManager dm: tm.dms.values()) {
            assertEquals(0, dm.snapshots.size());
        }

        // All up sites should have two snapshots after creating two transactions
        tm.run(stringToInputStream(instructions));
        assertEquals(2, tm.transactions.size());
        Transaction t1 = tm.transactions.get("T1");
        assertNotNull(t1);
        assertEquals("T1", t1.name);
        assertTrue(t1.isReadOnly);
        Transaction t2 = tm.transactions.get("T2");
        assertNotNull(t2);
        assertEquals("T2", t2.name);
        assertTrue(t2.isReadOnly);
        assertTrue(t1.beginTime < t2.beginTime);

        for (DataManager dm: tm.dms.values()) {
            assertEquals(2, dm.snapshots.size());
            assertNotNull(dm.snapshots.get(tm.ticks-1));
            assertNotNull(dm.snapshots.get(tm.ticks));
        }

        // If some sites are down, the readRO instruction should be blocked
        instructions = "beginRO(T3)\n";
        tm.siteStatusTable.put(1, new SiteStatus(RunningStatus.DOWN, tm.ticks));
        tm.run(stringToInputStream(instructions));
        assertEquals(2, tm.transactions.size());
        assertNull(tm.transactions.get("T3"));
        for (DataManager dm: tm.dms.values()) {
            assertEquals(2, dm.snapshots.size());
        }
    }

    @Test void testInstrFail() {
        TransactionManager tm = RepCRecDB.init();
        String instructions = "fail(1)";
        DataManager dm1 = tm.dms.get(1);
        dm1.lockTable.put(2, new LockEntry(LockType.READ ,"T1"));
        assertEquals(RunningStatus.UP, tm.siteStatusTable.get(dm1.siteID).status);
        assertEquals(1, dm1.lockTable.size());

        tm.run(stringToInputStream(instructions));
        assertEquals(RunningStatus.DOWN, tm.siteStatusTable.get(dm1.siteID).status);
        assertEquals(tm.ticks, tm.siteStatusTable.get(dm1.siteID).lastDownTime);
        assertEquals(0, dm1.lockTable.size());
    }

    @Test void testInstrRecover() {
        TransactionManager tm = RepCRecDB.init();
        DataManager dm1 = tm.dms.get(1);
        String instructions = "fail(1)";
        tm.run(stringToInputStream(instructions));
        int lastDownTime = tm.siteStatusTable.get(dm1.siteID).lastDownTime;
        assertEquals(tm.ticks, lastDownTime);
        assertEquals(RunningStatus.DOWN, tm.siteStatusTable.get(dm1.siteID).status);
        for (boolean readableStatus: dm1.repVarReadableTable.values()) {
            assertTrue(readableStatus);
        }

        String recoverInstr = "recover(1)";
        tm.run(stringToInputStream(recoverInstr));
        assertEquals(RunningStatus.UP, tm.siteStatusTable.get(dm1.siteID).status);
        assertEquals(lastDownTime, tm.siteStatusTable.get(dm1.siteID).lastDownTime);
        for (boolean readableStatus: dm1.repVarReadableTable.values()) {
            assertFalse(readableStatus);
        }
    }

    @Test void testInstrReadRO() {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        TransactionManager tm = RepCRecDB.init();
        tm.run(stringToInputStream("beginRO(T1)\nR(T1, x2)"));
        assertEquals("x2: 20\n", outContent.toString());
        assertEquals(1, tm.lastSiteID);

        tm.dms.get(2).dataTable.put(2, 30);
        tm.run(stringToInputStream("beginRO(T2)"));
        tm.siteStatusTable.put(tm.lastSiteID, new SiteStatus(RunningStatus.DOWN, tm.ticks));
        tm.run(stringToInputStream("R(T2, x2)"));
        assertEquals("x2: 20\nx2: 30\n", outContent.toString());
        assertEquals(2, tm.lastSiteID);

        System.setOut(System.out);
    }

    @Test void testInstrRead() {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        // Read replicated data
        TransactionManager tm = RepCRecDB.init();
        tm.run(stringToInputStream("begin(T1)\nR(T1, x2)"));
        assertEquals("x2: 20", getLastLineFromOutput(outContent.toString()));

        tm.dms.get(2).dataTable.put(2, 30);
        tm.run(stringToInputStream("begin(T2)\nR(T2, x2)"));
        assertEquals("x2: 30", getLastLineFromOutput(outContent.toString()));

        // Read non-replicated data
        tm.run(stringToInputStream("R(T1, x1)"));
        assertEquals("x1: 10", getLastLineFromOutput(outContent.toString()));

        tm.run(stringToInputStream("R(T2, x1)"));
        assertEquals("x1: 10", getLastLineFromOutput(outContent.toString()));

        tm.dms.get(2).lockTable.put(1, new LockEntry(LockType.WRITE, "T1"));
        tm.run(stringToInputStream("R(T1, x1)"));  // Should be blocked
        assertEquals(1, tm.instructionBuffer.size());

        tm.run(stringToInputStream("R(T2, x1)")); // Should be blocked
        assertEquals(2, tm.instructionBuffer.size());

        System.setOut(System.out);
    }

    @Test void testInstrWrite() {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        // Read-Write replicated data
        TransactionManager tm = RepCRecDB.init();
        tm.run(stringToInputStream("begin(T1)\nR(T1, x2)"));
        assertEquals("x2: 20", getLastLineFromOutput(outContent.toString()));
        int lastSiteID = tm.lastSiteID;
        tm.run(stringToInputStream("fail(1)\nW(T1, x2, 200)"));
        assertEquals(20, tm.dms.get(lastSiteID).dataTable.get(2));
        assertEquals(200, tm.transactions.get("T1").writes.get(0).value);
        assertEquals(9, tm.transactions.get("T1").writes.get(0).siteIDs.size());

        // Another T which reads x2 should be blocked
        assertEquals(0, tm.instructionBuffer.size());
        tm.run(stringToInputStream("begin(T2)\nR(T2, x2)"));
        assertEquals(1, tm.instructionBuffer.size());

        // Read-Write-Read non-replicated data
        outContent.reset();
        tm.run(stringToInputStream("R(T1, x1)"));
        assertTrue(outContent.toString().contains("x1: 10"));
        tm.run(stringToInputStream("W(T1, x1, 100)"));
        assertEquals(10, tm.dms.get(2).dataTable.get(1));
        assertEquals(100, tm.transactions.get("T1").writes.get(1).value);
        assertEquals(1, tm.transactions.get("T1").writes.get(1).siteIDs.size());

        // T Read-Write-Read data, the last Read reads from T's local write copy
        outContent.reset();
        tm.run(stringToInputStream("begin(T3)\nR(T3, x4)\n"));
        assertTrue(outContent.toString().contains("x4: 40"));
        tm.run(stringToInputStream("recover(1)\nW(T3, x4, 400)\nR(T3, x4)\n"));
        assertTrue(outContent.toString().contains("x4: 400"));
        assertEquals(10, tm.transactions.get("T3").writes.get(0).siteIDs.size());

        System.setOut(System.out);
    }

    @Test void testInstrEnd() {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));
        TransactionManager tm = RepCRecDB.init();

        // write -> end -> commits
        tm.run(stringToInputStream("begin(T1)\nW(T1, x2, 200)\nW(T1, x1, 100)\nend(T1)"));
        assertEquals("T1 commits", getLastLineFromOutput(outContent.toString()));

        // write -> fail -> recover -> end -> aborts
        tm.run(stringToInputStream("begin(T1)\nW(T1, x2, 200)\nfail(1)\nrecover(1)\nW(T1, x1, 100)\nend(T1)"));
        assertTrue(outContent.toString().contains("T1 aborts"));


        // 2 Transactions write different variable, commits in order
        tm = RepCRecDB.init();
        tm.run(stringToInputStream("begin(T1)\nbegin(T2)\nW(T2, x2, 200)\nW(T1, x1, 100)\nend(T1)\nend(T2)"));
        assertTrue(outContent.toString().contains("T1 commits\nT2 commits"));

        // 2 Transactions write the same variable, one blocks another
        // Commits in reversed order
        tm.run(stringToInputStream("begin(T1)\nbegin(T2)\nW(T2, x2, 200)\nW(T1, x2, 100)\nend(T1)\nend(T2)"));
        assertTrue(outContent.toString().contains("T2 commits\nT1 commits"));

        System.setOut(System.out);
    }

    @Test void testIntegration() {
        File[] files = new File("tests").listFiles();
        for (File file : files) {
            String filePath = file.getPath();
            if (filePath.endsWith(".in")) {
                String ansFilePath = filePath.replace(".in", ".ans");
                assertTrue(new File(ansFilePath).exists());

                System.out.println("Test File: " + filePath);
                System.out.println("Ans File: " + ansFilePath);

                // Print content of test file
                try(InputStream is = new FileInputStream(filePath);) {
                    try(Scanner input = new Scanner(is);) {
                        while (input.hasNext()) {
                            System.out.println(input.nextLine());
                        }
                    }
                } catch (Exception e) {
                    System.out.print(e);
                }

                // Run test
                try(InputStream is = new FileInputStream(filePath);) {
                    String contents = new String(Files.readAllBytes(Paths.get(ansFilePath)));
                    System.out.println(contents);
                    // Set output to a string
                    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
                    System.setOut(new PrintStream(outContent));
                    TransactionManager tm = RepCRecDB.init();
                    try {
                        tm.run(is);
                    } catch (Exception e) {
                        PrintStream consoleStream = new PrintStream(new FileOutputStream(FileDescriptor.out));
                        System.setOut(consoleStream);
                        System.out.println(e);
                        assert(false);
                    }
                    String res = outContent.toString();
                    assertEquals(contents.trim(), res.trim());
                    // Reset output to the console
                    PrintStream consoleStream = new PrintStream(new FileOutputStream(FileDescriptor.out));
                    System.setOut(consoleStream);
                } catch (Exception e)
                {
                    System.out.println(e);
                }
            }
        }
    }

    // *************************************
    //  U T I L I T Y   F U N C T I O N S 
    // *************************************
    InputStream stringToInputStream(String str) {
        return new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
    }

    String getLastLineFromOutput(String str) {
        String[] lines = str.split("\n");
        return lines[lines.length-1];
    }
}
