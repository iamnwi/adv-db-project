package repcrecdb;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

enum LockType
{ 
    READ, WRITE;
}

class LockEntry {
    public LockType lockType;
    public String transactionName;
    public boolean hasPendingWrite; // given W1(x1)R2(x1), avoid R2 read before W1

    public LockEntry(LockType lockType, String transactionName) {
        this.lockType = lockType;
        this.transactionName = transactionName;
        this.hasPendingWrite = false;
    }
}

public class DataManager {
    public int siteID;
    public HashMap<Integer, Integer> dataTable;
    public HashMap<Integer, LockEntry> lockTable; // (varName, (lock type, transName))
    public HashMap<Integer, Boolean> repVarReadableTable; // (varID, isReadable?)
    public TreeMap<Integer, HashMap<Integer, Integer>> snapshots; // (time, map(varID, val))

    public DataManager(int index) {
        siteID = index;
        lockTable = new HashMap<Integer, LockEntry>();
        snapshots = new TreeMap<Integer, HashMap<Integer, Integer>>();

        dataTable = new HashMap<Integer, Integer>();
        for (int i = 1; i <= 20; i++) {
            if (i % 2 == 0 || 1 + (i%10) == siteID) {
                dataTable.put(i, 10*i);
            }
        }

        // All replicated variables are readable at first
        repVarReadableTable = new HashMap<Integer, Boolean>();
        for (Integer varId : dataTable.keySet()) {
            if (varId % 2 == 0) {
                repVarReadableTable.put(varId, true);
            }
        }
    }
    
    public boolean acquireLock(String transactionName, int varID, LockType lockType) {
        if (!dataTable.containsKey(varID)) return false;

        boolean suc = false;
        LockEntry lockEntry = lockTable.get(varID);
        if (lockEntry == null 
        || (lockEntry.lockType == LockType.READ && !lockEntry.hasPendingWrite)) {
            lockTable.put(varID, new LockEntry(lockType, transactionName));
            suc = true;
        }
        else if (lockType == LockType.WRITE) {
            lockEntry.hasPendingWrite = true;
        }
        return suc;
    }

    public void fail() {
        lockTable.clear();
    }

    public void recover() {
        // Set all replicated variables as non-readable(write-only)
        for (Integer varId : repVarReadableTable.keySet()) {
            repVarReadableTable.put(varId, false);
        }
    }

    public Integer read(String transactionName, int varID) {
        LockEntry lockEntry = lockTable.get(varID);
        Integer val = null;
        if (lockEntry != null 
            && lockEntry.transactionName == transactionName 
            && (lockEntry.lockType == LockType.READ))
        {
            val = dataTable.get(varID);
        }
        return val;
    }

    public Integer readRO(int varID, int transBeginTime) {
        HashMap<Integer, Integer> snapshot = snapshots.get(transBeginTime);
        if (snapshot == null) return null;
        return snapshot.get(varID);
    }

    public void write() {
        // TODO: if the accessed var is a replicated variables, set its readability to TRUE
    }

    public void takeSnapshot(int currentTime) {
        snapshots.put(currentTime, dataTable);
    }

    public String queryState() {
        return this.toString();
    }

    public String toString() {
        TreeMap<Integer, Integer> sorted = new TreeMap<>(); 
        sorted.putAll(dataTable);
        StringBuilder dataStringBuilder = new StringBuilder();
        for (Map.Entry<Integer, Integer> entry : sorted.entrySet()) {
            dataStringBuilder.append(String.format("x%d: %d, ", entry.getKey(), entry.getValue()));
        }
        dataStringBuilder.delete(dataStringBuilder.length()-2, dataStringBuilder.length());
        return String.format("site %d - %s", siteID, dataStringBuilder.toString());
    }
}