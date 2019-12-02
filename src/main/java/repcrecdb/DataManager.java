package repcrecdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

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

    public boolean checkLock(String transactionName, int varID, LockType lockType) {
        LockEntry lockEntry = lockTable.get(varID);
        boolean suc = lockEntry == null
            || (lockEntry.lockType == LockType.READ && !lockEntry.hasPendingWrite)
            || (lockEntry.transactionName.equals(transactionName)
                && lockEntry.lockType == LockType.READ
                && lockType == LockType.WRITE);
        if (!suc && lockType == LockType.WRITE) {
            lockTable.get(varID).hasPendingWrite = true;
        }
        return suc;
    }

    public boolean acquireLock(String transactionName, int varID, LockType lockType) {
        if (!dataTable.containsKey(varID)) return false;
        if (checkLock(transactionName, varID, lockType)) {
            lockTable.put(varID, new LockEntry(lockType, transactionName));
            return true;
        }
        return false;
    }

    public void releaseLocks(String transactionName) {
        ArrayList<Integer> releasedVarIDs = new ArrayList<Integer>();
        for (Entry<Integer, LockEntry> entry: this.lockTable.entrySet()) {
            if (entry.getValue().transactionName.equals(transactionName)) {
                releasedVarIDs.add(entry.getKey());
            }
        }
        for (Integer varID: releasedVarIDs) {
            this.lockTable.remove(varID);
        }
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
            && lockEntry.transactionName.equals(transactionName)
            && lockEntry.lockType == LockType.READ)
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

    public boolean write(String transactionName, int varID, int val) {
        LockEntry lockEntry = lockTable.get(varID);
        if (lockEntry != null
            && lockEntry.transactionName.equals(transactionName)
            && lockEntry.lockType == LockType.WRITE)
        {
            dataTable.put(varID, val);
            // A replicated variable is non-readable after recovery
            // However, once we write it, it is readable then
            if (varID % 2 == 0) {
                repVarReadableTable.put(varID, true);
            }
            return true;
        }
        return false;
    }

    public void takeSnapshot(int currentTime) {
        HashMap<Integer, Integer> snapshot = new HashMap<Integer, Integer>();
        snapshot.putAll(dataTable);
        snapshots.put(currentTime, snapshot);
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