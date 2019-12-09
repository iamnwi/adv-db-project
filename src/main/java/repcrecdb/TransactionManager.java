package repcrecdb;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.Map.Entry;

enum RunningStatus
{ 
    UP, DOWN;
}

class SiteStatus {
    public RunningStatus status;
    public int lastDownTime;

    public SiteStatus(RunningStatus status, int currentTime) {
        this.status = status;
        this.lastDownTime = currentTime;
    }
}

public class TransactionManager {
    public static final String DEADLOCK_ABORT_MESSAGE = "Deadlock detected, younger killed";
    public static final String SITE_FAIL_ABORT_MESSAGE = "Accessed site(s) failed";

    HashMap<Integer, DataManager> dms;
    HashMap<String, Transaction> transactions;
    HashMap<Integer, SiteStatus> siteStatusTable;
    LinkedList<String> instructionBuffer;
    WaitForGraph waitForGraph;
    Integer ticks; // Mimic a ticking time
    int lastSiteID; // site ID from 1 to 10, workload balancing for replicated data

    /*
     * Description: initialize all fields 
     * Input: sites’ DM objects 
     * Output: N/A
     */
    public TransactionManager(HashMap<Integer, DataManager> dms) {
        ticks = 0;
        this.dms = dms;
        transactions = new HashMap<String, Transaction>();
        instructionBuffer = new LinkedList<String>();
        lastSiteID = dms.size();
        waitForGraph = new WaitForGraph();

        // Initialize the status for each site as up
        siteStatusTable = new HashMap<Integer, SiteStatus>();
        for (Integer siteID : dms.keySet()) {
            siteStatusTable.put(siteID, new SiteStatus(RunningStatus.UP, ticks));
        }
    }

    /*
     * Description: run all the instructions in a given text
     * Input: instructions text
     * Output: void
     * Side effect: 
     * Add one tick to time when meeting a newline
     * Parse the instructions text line by line
     * Append parsed instructions into command buffer one by one
     * Perform the instructions in the command buffer in orders at each tick
     */
    public void run(InputStream inputStream) {
        try (Scanner input = new Scanner(inputStream);) {
            while (!instructionBuffer.isEmpty() || input.hasNextLine()) {
                ticks += 1;

                // Detect deadlock at the start of ticks
                ArrayList<String> list = waitForGraph.detectDeadlock();
                if (list != null) {
                    String trancName = findYoungest(list);
                    abort(trancName, DEADLOCK_ABORT_MESSAGE);
                }

                // Add new instruction into buffer
                boolean hasNewInstr = input.hasNextLine();
                if (hasNewInstr) {
                    String instr = input.nextLine().trim();
                    // Ignore comment lines and empty lines
                    if (instr.startsWith("//") || instr.length() == 0) {
                        ticks -= 1;
                        continue;
                    }
                    instructionBuffer.add(instr);
                }

                // Execute instructions in instruction buffer until one that 
                // is not blocked
                boolean allBlocked = true;
                for (int i = 0; i < instructionBuffer.size(); i++) {
                    String instr = instructionBuffer.get(i);
                    if (parse(instr, i < instructionBuffer.size()-1 || !hasNewInstr)) {
                        instructionBuffer.remove(i);
                        allBlocked = false;
                        break;
                    }
                }
                if (allBlocked && !input.hasNextLine()) {
                    System.out.println("All following instructions are blocked");
                    System.out.println(instructionBuffer.toString());
                    break;
                }
            }
        }
    }

    /**
     * Description: Parse a given instruction
     * Input: one instruction in string, is blocked or not
     * Output: instruction succeeds or not
     */
    public boolean parse(String instruction, boolean isBlocked) {
        // Parse command and args
        String[] tokens = instruction.replaceAll("\\)", "").split("\\(");
        String command = tokens[0];
        String[] args = new String[0];
        if (tokens.length > 1) {
            args = tokens[1].replaceAll(" ", "").split(",");
        }

        // Dispatch
        String tName = null;
        boolean suc = false;
        switch (command) {
            case "begin":
                return (args.length == 1) && begin(args[0]);
            case "beginRO":
                return (args.length == 1) && beginRO(args[0]);
            case "R":
                tName = args[0];
                suc = (args.length == 2) && read(tName, args[1]);
                updateBlockedInstrCnt(tName, suc, isBlocked);
                return suc;
            case "W":
                tName = args[0];
                suc = (args.length == 3) && write(tName, args[1], Integer.parseInt(args[2]));
                updateBlockedInstrCnt(tName, suc, isBlocked);
                return suc;
            case "dump":
                return dump();
            case "end":
                return (args.length == 1) && end(args[0]);
            case "fail":
                return (args.length == 1) && fail(Integer.parseInt(args[0]));
            case "recover":
                return (args.length == 1) && recover(Integer.parseInt(args[0]));
            case "queryState":
                return queryState();
            default:
                System.out.println("Unknown instruction");
                return false;
        }
    }

    /**
     * Description: handle transaction begin instruction
     * Input: transaction name
     * Output: succeed or not
     * Side effect: 
     * Create Transaction with begin time and type, append to TM transaction list
     */
    public boolean begin(String transactionName) {
        transactions.put(transactionName, new Transaction(transactionName, ticks));
        return true;
    }

    /**
     * Description: handle read-only transaction begin instruction
     * Input: transaction name
     * Output: succeed or not
     * Side effect: 
     * Create Transaction with begin time and type(RO), append to TM transaction list
     * Notify all up sites to create a snapshot with a given timestamp(the begin time of this Transaction)
     * Append to the command buffer if blocked(all sites are down, no one can create a snapshot)
     */
    public boolean beginRO(String transactionName) {
        int downCnt = siteStatusTable.size() - getUpSiteCount();
        if (downCnt > 0) return false;

        for (Entry<Integer, SiteStatus> statusEntry: siteStatusTable.entrySet()) {
            dms.get(statusEntry.getKey()).takeSnapshot(ticks);
        }
        transactions.put(transactionName, new Transaction(transactionName, ticks, true));
        return true;
    }

    /**
     * Description: handle transaction read data instruction
     * Input: transaction name, variable name
     * Output: succeed or not
     * Side effect: 
     * If acquire read lock success:
     *      Notify the accessed site to update its locks table
     *      Print the variable value in the format “x{number}: {val}”
     *      Add accessed site to Transaction’s access sites set
     * If fail(no sites can provide the read lock for the target variable):
     *      Add one edge to the wait-for graph if it is a read-write Transaction
     *      Append to the command buffer if blocked
     *      Set check deck-lock to true to perform a deadlock at next tick
     * Update the pointer of next available site
     */
    public boolean read(String transactionName, String varName) {
        Transaction t = transactions.get(transactionName);
        if (t == null) {
            return true;
        }
        int varID = Integer.parseInt(varName.substring(1));
        boolean isReplicatedData = varID % 2 == 0;
        int siteID = -1;
        if (isReplicatedData) {
            siteID = findNextSite();
        } else {
            int targetSiteID = (varID % 10) + 1;
            if (siteStatusTable.get(targetSiteID).status == RunningStatus.UP) {
                siteID = targetSiteID;
            }
        }
        if (siteID == -1) return false;

        Integer val = null;
        int upCnt = getUpSiteCount();
        int tryCnt = 1;
        HashSet<String> blockTrancSet = new HashSet<>();
        while (val == null) {
            DataManager dm = dms.get(siteID);
            if (t.isReadOnly) {
                val = dm.readRO(varID, t.beginTime);
            } else {
                // Try to read from local writes firstlockTable
                // For situation W(T1, x1, 10)R(T1, x1), R should read the local write value of T1.
                val = this.transactions.get(transactionName).read(varID);

                if (val == null) {
                    if (!isReplicatedData || dm.repVarReadableTable.getOrDefault(varID, false)) {
                        blockTrancSet = dm.acquireLock(transactionName, varID, LockType.READ);
                        if (blockTrancSet.isEmpty()) {
                            val = dm.read(transactionName, varID);
                        }
                    }
                }
            }
            if (val != null || !isReplicatedData || tryCnt >= upCnt) break;
            siteID = findNextSite();
            tryCnt += 1;
        }
        
        if (val != null) {
            System.out.println(String.format("%s: %d", varName, val));
            if (!t.accessedSites.containsKey(siteID)) {
                t.accessedSites.put(siteID, this.ticks);
            }
        }

        for (String tranc : blockTrancSet) {
            if (!transactionName.equals(tranc)) {
                waitForGraph.addEdge(transactionName, tranc);
            }
        }

        return !(val == null);
    }

    /**
     * Description: handle transaction write data instruction
     * Input: transaction name, variable name, the new variable value
     * Output: succeed or not
     * Side effect:
     * Append this command to Transaction’s write command
     * If acquire write locks success:
     *      Notify all up sites to update its locks table
     *      Add accessed site to Transaction’s access sites set
     * If fail:
     *      Add edge(s) to the wait-for graph
     *      Append to the command buffer
     *      Set check deck-lock to true to perform a deadlock at next tick
     * Update the pointer of next available site
     */
    public boolean write(String transactionName, String varName, int val) { 
        int varID = Integer.parseInt(varName.substring(1));
        boolean suc = false;
        Transaction t = this.transactions.get(transactionName);
        if (t == null) {
            return true;
        }
        int upCnt = this.getUpSiteCount();
        if (upCnt == 0) return false;

        HashSet<String> blockTrancSet = new HashSet<>();
        if (varID % 2 == 0)
        {
            // Acquired write locks from every up site for even index variables
            int acquireLockCnt = 0;
            for (Entry<Integer, SiteStatus> entry: siteStatusTable.entrySet()) {
                int siteID = entry.getKey();
                DataManager dm = dms.get(siteID);
                if (entry.getValue().status == RunningStatus.UP) {
                    HashSet<String> blockTrancSetTmp = dm.checkLock(transactionName, varID, LockType.WRITE);
                    if (blockTrancSetTmp.isEmpty()) {
                        acquireLockCnt++;
                    } else {
                        blockTrancSet = blockTrancSetTmp;
                    }
                }
            }
            if (acquireLockCnt == upCnt) {
                WriteRecord writeRec = new WriteRecord(varID, val);
                for (Entry<Integer, SiteStatus> entry: siteStatusTable.entrySet()) {
                    int siteID = entry.getKey();
                    DataManager dm = dms.get(siteID);
                    if (entry.getValue().status == RunningStatus.UP)
                    {
                        dm.acquireLock(transactionName, varID, LockType.WRITE);
                        writeRec.siteIDs.add(siteID);
                        if (!t.accessedSites.containsKey(siteID)) {
                            t.accessedSites.put(siteID, this.ticks);
                        }
                    }
                }

                t.writes.add(writeRec);  // Write to local copy of T, write to site on commit
                suc = true;
            } else {
                for (Entry<Integer, SiteStatus> entry: siteStatusTable.entrySet()) {
                    int siteID = entry.getKey();
                    DataManager dm = dms.get(siteID);
                    if (entry.getValue().status == RunningStatus.UP)
                    {
                        dm.setPendingWrite(transactionName, varID);
                    }
                }
            }
        } else {
            // Acquired write lock from the target site for odd index variables
            int siteID = (varID % 10) + 1;
            DataManager dm = dms.get(siteID);
            if (siteStatusTable.get(siteID).status == RunningStatus.UP) {
                blockTrancSet = dm.acquireLock(transactionName, varID, LockType.WRITE);
                if (blockTrancSet.isEmpty()) {
                    WriteRecord writeRec = new WriteRecord(varID, val);
                    writeRec.siteIDs.add(siteID);
                    t.writes.add(writeRec);  // Write to local copy of T, write to site on commit
                    if (!t.accessedSites.containsKey(siteID)) {
                        t.accessedSites.put(siteID, this.ticks);
                    }
                    suc = true;
                }
            }
        }

        if (!suc) {
            for (String tranc : blockTrancSet) {
                if (!transactionName.equals(tranc)) {
                    waitForGraph.addEdge(transactionName, tranc);
                }
            }
        }
        return suc;
    }

   /**
    * Description: handle transaction write data instruction 
    * Input: N/A
    * Output: succeed or not
    * Side effect:
    * Print data tables of all sites(no matter down or up)
    */
    public boolean dump() {
        for (DataManager dm : dms.values()) {
            System.out.println(dm.toString());
        }
        return true;
    }

    /**
     * Description: Perform a commit validation to see if the given transaction can be committed. If it is a read-write transaction, release all the locks it holds.
     * Input: transaction name
     * Output: succeed or not
     * Side effect: 
     * Print whether the given transaction is committed or aborted.
     * Update lock tables of sites that the given transaction accessed.
     * Update wait-for graph(remove edges related to the given transaction)
     * If can commit, update data tables of sites that the given transaction accessed.
     */
    public boolean end(String transactionName) {
        // Check if the sites have been down after T accessed them
        boolean commit = true;
        Transaction t = this.transactions.get(transactionName);

        // If this "begin" instruction of this T is blocked,
        // then we will not find records of this T
        if (t == null) {
            return true;
        }
        if (t.blockedInstrCnt > 0) {
            return false;
        }

        for (Entry<Integer, Integer> entry: t.accessedSites.entrySet()) {
            int siteID = entry.getKey();
            int accessTime = entry.getValue();
            if (this.siteStatusTable.get(siteID).lastDownTime > accessTime) {
                commit = false;
                break;
            }
        }
        if (commit) {
            // If a T has write operations, then T must have accessed to and 
            // hold write locks from all up sites at the moment of the write
            // operation issued.
            for (WriteRecord writeRec: t.writes) {
                for (Integer siteID: writeRec.siteIDs) {
                    DataManager dm = this.dms.get(siteID);
                    boolean suc = dm.write(transactionName, writeRec.varID, writeRec.value);
                    assert(suc == true);
                }
            }
        }
        for (int siteID: t.accessedSites.keySet()) {
            DataManager dm = this.dms.get(siteID);
            dm.releaseLocks(transactionName);
        }
        this.transactions.remove(transactionName);
        waitForGraph.removeNode(transactionName);
        if (commit) {
            System.out.println(String.format("%s commits", transactionName));
        } else {
            System.out.println(String.format("%s aborts(%s)", transactionName, SITE_FAIL_ABORT_MESSAGE));
        }

        return true;
    }

    /**
     * Description: Abort one transaction
     * Input: transaction name, abort reason message
     * Output: void
     * Side effect: 
     * Print whether the given transaction is  aborted.
     * Update lock tables of sites that the given transaction accessed.
     * Update wait-for graph(remove edges related to the given transaction)
     */
    private void abort(String transactionName, String message) {
        Transaction t = this.transactions.get(transactionName);
        for (int siteID: t.accessedSites.keySet()) {
            DataManager dm = this.dms.get(siteID);
            dm.releaseLocks(transactionName);
        }

        // Remove buffered instructions related to this aborted Transaction
        Iterator<String> it = instructionBuffer.iterator();
        while (it.hasNext()) {
            String instr = it.next();
            if (instr.contains(transactionName)){
                it.remove();
            }
        }

        waitForGraph.removeNode(transactionName);
        this.transactions.remove(transactionName);
        System.out.println(String.format("%s aborts(%s)", transactionName, message));
    }

    /**
     * Description: Mimic the situation that the given site is down
     * Input: site id
     * Output: succeed or not
     * Side effect: 
     * Set the status of the given site as down in the site status table
     * Notify the given site to down
     */
    public boolean fail(Integer siteID) {
        siteStatusTable.put(siteID, new SiteStatus(RunningStatus.DOWN, ticks));
        dms.get(siteID).fail();
        return true;
    }

    /**
     * Description: Mimic the situation that the given site is recovered from a failure
     * Input: site id
     * Output: succeed or not
     * Side effect:
     * Set the status of the given site as up
     * Notify the given site to recover
     */
    public boolean recover(Integer siteID) {
        int lastDownTime = siteStatusTable.get(siteID).lastDownTime;
        siteStatusTable.put(siteID, new SiteStatus(RunningStatus.UP, lastDownTime));
        dms.get(siteID).recover();
        return true;
    }

    /**
     * Description: Print out all status of TM and each DM
     * Input: void
     * Output: succeed or not
     * Side effect:
     * Print all status in TM
     * Print all status and data in each DM
     */
    public boolean queryState() {
        System.out.println(String.join("", Collections.nCopies(70, "-")));
        System.out.println("Transaction Manager");
        System.out.println(String.join("", Collections.nCopies(70, "-")));

        System.out.println(this.toString());

        System.out.println(String.join("", Collections.nCopies(70, "-")));
        System.out.println("Data Managers");
        System.out.println(String.join("", Collections.nCopies(70, "-")));

        for (DataManager dm : dms.values()) {
            System.out.println(dm.toString());
        }

        return true;
    }

    // *************************************
    //  U T I L I T Y   F U N C T I O N S 
    // *************************************

    /**
     *
     * Description: Update the counter for blocked instructions for each transaction
     * Input: transaction name, succeed or not, is blocked or not
     * Output: void
     * Side effect:
     * Increase counter if not succeed and not blocked
     * Decrease counter if succeed and blocked
     */
    public void updateBlockedInstrCnt(String tName, boolean suc, boolean isBlocked) {
        Transaction t = this.transactions.get(tName);
        if (t == null) {
            return;
        }

        if (!suc) {
            // If not success, add up T's blocked instruction if the
            // current executed instruction is not yet count as a blocked instruction
            if (!isBlocked) {
                t.blockedInstrCnt += 1;
            }
        } else {
            if (isBlocked) {
                t.blockedInstrCnt -= 1;
            }
        }
    }

    /**
     * Description: find ID of next site to balance the workload of replicated data accessing
     * Input: N/A
     * Output: ID of next site
     * Side effect: N/A
     */
    public int findNextSite() {
        int maxSiteID = dms.size();
        int tryCnt = 0;
        do {
            if (tryCnt == maxSiteID) {
                return -1;
            }
            lastSiteID += 1;
            if (lastSiteID > maxSiteID) lastSiteID = 1;
            tryCnt += 1;
        } while (siteStatusTable.get(lastSiteID).status == RunningStatus.DOWN);
        return lastSiteID;
    }

    /**
     * Description: count how many sites are up
     * Input: N/A
     * Output: count of up sites
     * Side effect: N/A
     */
    public int getUpSiteCount() {
        int upCnt = 0;
        for (SiteStatus status: siteStatusTable.values()) {
            if (status.status == RunningStatus.UP) upCnt += 1;
        }
        return upCnt;
    }

    /**
     * Description: convert transaction state to string
     * Input: N/A
     * Output: transaction state in string
     * Side effect: N/A
     */
    public String toString() {
        StringBuilder state = new StringBuilder();
        state.append("Transactions\n");
        for (Transaction t : transactions.values()) {
            state.append(String.format("- Name: %s%s\tBegin Time: %s\n", t.name, t.isReadOnly ? "(RO)" : "", t.beginTime));
        }
        state.append("\nInstruction Buffer\n");
        for (String instr : instructionBuffer) {
            state.append(String.format("- %s\n", instr));
        }
        return state.toString();
    }

    /**
     * Description: find the youngest transaction in a loop
     * Input: array list containing all transactions' names in the loop
     * Output: transaction name of the youngest in the loop
     * Side effect: N/A
     */
    private String findYoungest(ArrayList<String> list) {
        int maxBegin = Integer.MIN_VALUE;
        String maxName = "";
        for (String tranc : list) {
            if (transactions.get(tranc).beginTime > maxBegin) {
                maxBegin = transactions.get(tranc).beginTime;
                maxName = tranc;
            }
        }
        return maxName;
    }
}