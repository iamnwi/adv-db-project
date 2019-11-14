package repcrecdb;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
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
    HashMap<Integer, DataManager> dms;
    HashMap<String, Transaction> transactions;
    HashMap<Integer, SiteStatus> siteStatusTable;
    LinkedList<String> instructionBuffer;
    Integer ticks; // Mimic a ticking time
    int nextSiteID; // site ID from 1 to 10, workload balancing for replicated data

    public TransactionManager(HashMap<Integer, DataManager> dms) {
        ticks = 0;
        this.dms = dms;
        transactions = new HashMap<String, Transaction>();
        instructionBuffer = new LinkedList<String>();
        nextSiteID = 1;

        // Initialize the status for each site as up
        siteStatusTable = new HashMap<Integer, SiteStatus>();
        for (Integer siteID : dms.keySet()) {
            siteStatusTable.put(siteID, new SiteStatus(RunningStatus.UP, ticks));
        }
    }

    public void run(InputStream inputStream) {
        Scanner input = new Scanner(inputStream);
        try {
            while (!instructionBuffer.isEmpty() || input.hasNext()) {
                instructionBuffer.add(input.nextLine());
                ticks += 1;
                // Execute instructions in instruction buffer until one that 
                // is not blocked
                boolean allBlocked = true;
                for (int i = 0; i < instructionBuffer.size(); i++) {
                    String instr = instructionBuffer.get(i);
                    if (parse(instr)) {
                        instructionBuffer.remove(i);
                        allBlocked = false;
                        break;
                    }
                }
                if (allBlocked && !input.hasNext()) {
                    System.out.println("All following instructions are blocked");
                    System.out.println(instructionBuffer.toString());
                    break;
                }
            }
        }
        finally {
            input.close();
        }
    }

    public boolean parse(String instruction) {
        // Parse command and args
        String[] tokens = instruction.replaceAll("\\)", "").split("\\(");
        String command = tokens[0];
        String[] args = new String[0];
        if (tokens.length > 1) {
            args = tokens[1].replaceAll(" ", "").split(",");
        }

        // Dispatch
        switch (command) {
            case "begin":
                return (args.length == 1) && begin(args[0]);
            case "beginRO":
                return (args.length == 1) && beginRO(args[0]);
            case "R":
                return (args.length == 2) && read(args[0], args[1]);
            case "W":
                return (args.length == 3) && write(args[0], args[1], args[2]);
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

    public boolean begin(String transactionName) {
        transactions.put(transactionName, new Transaction(transactionName, ticks));
        return true;
    }

    public boolean beginRO(String transactionName) {
        int downCnt = siteStatusTable.size() - getUpSiteCount();
        if (downCnt > 0) return false;

        for (Entry<Integer, SiteStatus> statusEntry: siteStatusTable.entrySet()) {
            dms.get(statusEntry.getKey()).takeSnapshot(ticks);
        }
        transactions.put(transactionName, new Transaction(transactionName, ticks, true));
        return true;
    }

    public boolean read(String transactionName, String varName) {
        /*
            TODO:
            For situation W(T1, x1, 10)R(T1, x1), R should be blocked until T1 commits.
            So, we can add all instructions of T1 that blocked by its first W operator into T1's
            instructions and execute them at once when T1 commits.
        */
        Transaction t = transactions.get(transactionName);
        if (t == null) return false;
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
        while (val == null) {
            if (t.isReadOnly) {
                val = dms.get(siteID).readRO(varID, t.beginTime);
            } else {
                boolean acquireLockSuc = dms.get(siteID).acquireLock(transactionName ,varID, LockType.READ);
                if (acquireLockSuc) {
                    val = dms.get(siteID).read(transactionName, varID);
                }
            }
            if (!isReplicatedData || tryCnt >= upCnt) break;
            siteID = findNextSite();
            tryCnt += 1;
        }
        
        if (val != null) {
            System.out.println(String.format("%s: %d", varName, val));
            if (isReplicatedData) {
                nextSiteID = siteID+1;
            }
        }

        return !(val == null);
    }

    public boolean write(String transactionName, String varName, String val) { return true; }

    public boolean dump() {
        for (DataManager dm : dms.values()) {
            System.out.print(dm.toString());
        }
        return true;
    }

    public boolean end(String transactionName) { return true; }

    public boolean fail(Integer siteID) {
        siteStatusTable.put(siteID, new SiteStatus(RunningStatus.DOWN, ticks));
        dms.get(siteID).fail();
        return true;
    }

    public boolean recover(Integer siteID) {
        int lastDownTime = siteStatusTable.get(siteID).lastDownTime;
        siteStatusTable.put(siteID, new SiteStatus(RunningStatus.UP, lastDownTime));
        dms.get(siteID).recover();
        return true;
    }

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

    public void detectDeadLock() {}

    // *************************************
    //  U T I L I T Y   F U N C T I O N S 
    // *************************************

    // Balance the workload of replicated data accessing
    public int findNextSite() {
        int maxSiteID = 10;
        int tryCnt = 0;
        while (siteStatusTable.get(nextSiteID).status == RunningStatus.DOWN) {
            nextSiteID += 1;
            if (nextSiteID > maxSiteID) nextSiteID = 1;
            tryCnt += 1;
            if (tryCnt == maxSiteID) {
                return -1;
            }
        }
        return nextSiteID;
    }

    public int getUpSiteCount() {
        int upCnt = 0;
        for (SiteStatus status: siteStatusTable.values()) {
            if (status.status == RunningStatus.UP) upCnt += 1;
        }
        return upCnt;
    }

    public String toString() {
        StringBuilder state = new StringBuilder();
        state.append("Transactions\n");
        for (Transaction t : transactions.values()) {
            state.append(String.format("- Name: %s%s\tBegin Time: %s\n", t.name, t.isReadOnly ? "(RO)" : "", t.beginTime));
        }
        return state.toString();
    }
}