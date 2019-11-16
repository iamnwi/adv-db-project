package repcrecdb;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.Map.Entry;

import javax.xml.crypto.Data;

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
            while (!instructionBuffer.isEmpty() || input.hasNextLine()) {
                boolean hasNewInstr = input.hasNextLine();
                if (hasNewInstr) {
                    instructionBuffer.add(input.nextLine());
                }
                ticks += 1;
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
        finally {
            input.close();
        }
    }

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
            DataManager dm = dms.get(siteID);
            if (t.isReadOnly) {
                val = dm.readRO(varID, t.beginTime);
            } else {
                boolean acquireLockSuc = dm.acquireLock(transactionName ,varID, LockType.READ);
                if (acquireLockSuc) {
                    val = dm.read(transactionName, varID);
                }
            }
            // For situation W(T1, x1, 10)R(T1, x1), R should read the local write value of T1.
            if (val == null) {
                LockEntry lockEntry = dm.lockTable.get(varID);
                if (lockEntry.transactionName.equals(transactionName)
                    && lockEntry.lockType == LockType.WRITE)
                {
                    val = this.transactions.get(transactionName).read(varID);
                }
            }
            if (!isReplicatedData || tryCnt >= upCnt) break;
            siteID = findNextSite();
            tryCnt += 1;
        }
        
        if (val != null) {
            System.out.println(String.format("%s: %d", varName, val));
            t.accessedSites.add(siteID);
            if (isReplicatedData) {
                nextSiteID = siteID+1;
            }
        }

        return !(val == null);
    }

    public boolean write(String transactionName, String varName, int val) { 
        int varID = Integer.parseInt(varName.substring(1));
        boolean suc = false;
        Transaction t = this.transactions.get(transactionName);

        if (varID % 2 == 0)
        {
            // Acquired write locks from every up site for even index variables
            int accessibleLocks = 0;
            for (Entry<Integer, SiteStatus> entry: siteStatusTable.entrySet()) {
                DataManager dm = dms.get(entry.getKey());
                if (entry.getValue().status == RunningStatus.UP 
                    && dm.checkLock(transactionName, varID, LockType.WRITE))
                {
                    accessibleLocks += 1;
                }
            }
            if (accessibleLocks == siteStatusTable.size()) {
                suc = true;
                for (Integer siteID: siteStatusTable.keySet()) {
                    DataManager dm = dms.get(siteID);
                    dm.acquireLock(transactionName, varID, LockType.WRITE);
                    t.accessedSites.add(siteID);
                }
                // Write to local copy of T, write to site on commit
                t.writes.put(varID, val);
            }
        } else {
            // Acquired write lock from the target site for odd index variables
            int siteID = (varID % 10) + 1;
            DataManager dm = dms.get(siteID);
            if (siteStatusTable.get(siteID).status == RunningStatus.UP
                && dm.acquireLock(transactionName, varID, LockType.WRITE))
            {
                suc = true;
                t.writes.put(varID, val);
                t.accessedSites.add(siteID);
            }
        }

        return suc;
    }

    public boolean dump() {
        for (DataManager dm : dms.values()) {
            System.out.print(dm.toString());
        }
        return true;
    }

    public boolean end(String transactionName) {
        // Check if the sites have been down after T accessed them
        boolean commit = true;
        Transaction t = this.transactions.get(transactionName);

        // If this "begin" instruction of this T is blocked,
        // then we will not find records of this T
        if (t == null || t.blockedInstrCnt > 0) return false;

        for (int siteID: t.accessedSites) {
            if (this.siteStatusTable.get(siteID).lastDownTime > t.beginTime) {
                commit = false;
                break;
            }
        }
        if (commit) {
            // If a T has write operations, then T must have accessed to all the sites
            // and is holding all the write locks from all the sites. If any of the sites
            // is down currently, we will not arrive at this step. So we do not need to 
            // check the status of all the sites
            for (Entry<Integer, Integer> entry: t.writes.entrySet()) {
                int varID = entry.getKey();
                int val = entry.getValue();
                if (varID % 2 == 0) {
                    for (DataManager dm: this.dms.values()) {
                        boolean suc = dm.write(transactionName, varID, val);
                        assert(suc == true);
                    }
                }
                else {
                    dms.get((varID % 10)+1).write(transactionName, varID, val);
                }
            }
        }
        for (int siteID: t.accessedSites) {
            DataManager dm = this.dms.get(siteID);
            dm.releaseLocks(transactionName);
        }
        this.transactions.remove(transactionName);
        System.out.println(String.format("%s %s", transactionName, commit ? "commits" : "aborts"));
        return commit;
    }

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

    public void updateBlockedInstrCnt(String tName, boolean suc, boolean isBlocked) {
        Transaction t = this.transactions.get(tName);
        assert(t != null);

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