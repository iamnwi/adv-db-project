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

    public TransactionManager(HashMap<Integer, DataManager> dms) {
        ticks = 0;
        this.dms = dms;
        transactions = new HashMap<String, Transaction>();
        instructionBuffer = new LinkedList<String>();

        // Initialize the status for each site as up
        siteStatusTable = new HashMap<Integer, SiteStatus>();
        for (Integer siteID : dms.keySet()) {
            siteStatusTable.put(siteID, new SiteStatus(RunningStatus.UP, ticks));
        }
    }

    public void run(InputStream inputStream) {
        // Read all commands into command buffer
        Scanner input = new Scanner(inputStream);
        try {
            while (input.hasNext()) {
                instructionBuffer.add(input.nextLine());
            }
        } finally {
            input.close();
        }

        // Execute instructions in instruction buffer until one that 
        // is not blocked
        boolean allBlocked = true;
        while (!instructionBuffer.isEmpty()) {
            for (int i = 0; i < instructionBuffer.size(); i++) {
                String instr = instructionBuffer.get(i);
                ticks += 1;
                if (parse(instr)) {
                    instructionBuffer.remove(i);
                    allBlocked = false;
                    break;
                } else {
                    ticks -= 1;
                }
            }
            if (allBlocked) {
                System.out.println("All following instructions are blocked");
                System.out.println(instructionBuffer.toString());
                return;
            }
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
            case "read":
                return (args.length == 2) && read(args[0], args[1]);
            case "write":
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
        boolean suc = false;
        for (Entry<Integer, SiteStatus> statusEntry: siteStatusTable.entrySet()) {
            if (statusEntry.getValue().status == RunningStatus.UP) {
                suc = true;
                dms.get(statusEntry.getKey()).takeSnapshot(ticks);
            }
        }
        if (suc) {
            transactions.put(transactionName, new Transaction(transactionName, ticks, true));
        }
        return suc;
    }

    public boolean read(String transactionName, String varName) { return true; }

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

    public String toString() {
        StringBuilder state = new StringBuilder();
        state.append("Transactions\n");
        for (Transaction t : transactions.values()) {
            state.append(String.format("- Name: %s%s\tBegin Time: %s\n", t.name, t.isReadOnly ? "(RO)" : "", t.beginTime));
        }
        return state.toString();
    }
}