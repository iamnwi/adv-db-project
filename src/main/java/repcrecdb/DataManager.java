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

    public LockEntry(LockType lockType, String transactionName) {
        this.lockType = lockType;
        this.transactionName = transactionName;
    }
}

public class DataManager {
    public int siteID;
    public HashMap<Integer, Integer> dataTable;
    public HashMap<Integer, LockEntry> lockTable; // (varName, lock entry)
    public HashMap<Integer, String> pendingWriteTable; // (varName, pend write transaction name)
    public HashMap<Integer, Boolean> repVarReadableTable; // (varID, isReadable?)
    public TreeMap<Integer, HashMap<Integer, Integer>> snapshots; // (time, map(varID, val))

    public DataManager(int index) {
        siteID = index;
        lockTable = new HashMap<Integer, LockEntry>();
        pendingWriteTable = new HashMap<Integer, String>();
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

    // checkLock() returns null if lock is available or another transaction's name if it is blocked
    public String checkLock(String transactionName, int varID, LockType lockType) {
        LockEntry lockEntry = lockTable.get(varID);
        String pendingWriteTran = pendingWriteTable.get(varID);
        boolean suc = 
            // No lock entry and no pending write 
            // or this transaction is the pending one
            (lockEntry == null
                && (pendingWriteTran == null
                    || (pendingWriteTran != null
                        && lockType ==  LockType.WRITE
                        && pendingWriteTran.equals(transactionName)))
            )
            // Both are read lock and no pending write
            || (lockEntry != null
                && lockEntry.lockType == LockType.READ
                && lockType == LockType.READ
                && (pendingWriteTran == null || lockEntry.transactionName.equals(transactionName)))
            // Promote read lock to write lock
            || (lockEntry != null
                && lockEntry.transactionName.equals(transactionName)
                && lockEntry.lockType == LockType.READ
                && lockType == LockType.WRITE);
        return suc ? null : lockEntry == null ? pendingWriteTran : lockEntry.transactionName;
    }

    public String acquireLock(String transactionName, int varID, LockType lockType) {
        if (!dataTable.containsKey(varID)) return "";
        String checkLockBlock = checkLock(transactionName, varID, lockType);
        if (checkLockBlock == null) {
            lockTable.put(varID, new LockEntry(lockType, transactionName));
            String pendingWriteTran = pendingWriteTable.get(varID);
            if (lockType == LockType.WRITE
                && pendingWriteTran != null
                && pendingWriteTran.equals(transactionName)) {
                pendingWriteTable.remove(varID);
            }
            return null;
        }
        return checkLockBlock;
    }

    public boolean setPendingWrite(String transactionName, int varID) {
        if (pendingWriteTable.get(varID) == null) {
            pendingWriteTable.put(varID, transactionName);

            // Remove read lock of from the same Transaction
            // as it should read the new value afterwards.
            // Otherwise, it will read the old value.
            LockEntry lockEntry = lockTable.get(varID);
            if (lockEntry != null
                && lockEntry.lockType == LockType.READ
                && lockEntry.transactionName.equals(transactionName))
            {
                lockTable.remove(varID);
            }
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
        pendingWriteTable.clear();
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