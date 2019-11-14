package repcrecdb;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Scanner;

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
    Integer ticks; // Mimic a ticking time

    public TransactionManager(HashMap<Integer, DataManager> dms) {
        ticks = 0;
        this.dms = dms;
        transactions = new HashMap<String, Transaction>();

        // Initialize the status for each site as up
        siteStatusTable = new HashMap<Integer, SiteStatus>();
        for (Integer siteID : dms.keySet()) {
            siteStatusTable.put(siteID, new SiteStatus(RunningStatus.UP, ticks));
        }
    }

    public void run(InputStream inputStream) {
        Scanner input = new Scanner(inputStream);
        try {
            while (input.hasNext()) {
                ticks += 1;
                parse(input.nextLine());
            }
        } finally {
            input.close();
        }
    }

    public void parse(String instruction) {
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
                if (args.length == 1)
                    begin(args[0]);
                break;
            case "beginRO":
                if (args.length == 1)
                    beginRO(args[0]);
                break;
            case "read":
                if (args.length == 2)
                    read(args[0], args[1]);
                break;
            case "write":
                if (args.length == 3)
                    write(args[0], args[1], args[2]);
                break;
            case "dump":
                dump();
                break;
            case "end":
                if (args.length == 1)
                    end(args[0]);
                break;
            case "fail":
                if (args.length == 1)
                    fail(Integer.parseInt(args[0]));
                break;
            case "recover":
                if (args.length == 1)
                    recover(Integer.parseInt(args[0]));
                break;
            case "queryState":
                queryState();
                break;
            default:
                System.out.println("Unknown instruction");
        }
    }

    public void begin(String transactionName) {
        transactions.put(transactionName, new Transaction(transactionName, ticks));
    }

    public void beginRO(String transactionName) {
        transactions.put(transactionName, new Transaction(transactionName, ticks, true));
    }

    public void read(String transactionName, String varName) {}

    public void write(String transactionName, String varName, String val) {}

    public void dump() {
        for (DataManager dm : dms.values()) {
            System.out.print(dm.toString());
        }
    }

    public void end(String transactionName) {}

    public void fail(Integer siteID) {
        siteStatusTable.put(siteID, new SiteStatus(RunningStatus.DOWN, ticks));
        dms.get(siteID).fail();
    }

    public void recover(Integer siteID) {
        int lastDownTime = siteStatusTable.get(siteID).lastDownTime;
        siteStatusTable.put(siteID, new SiteStatus(RunningStatus.UP, lastDownTime));
        dms.get(siteID).recover();
    }

    public void queryState() {
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