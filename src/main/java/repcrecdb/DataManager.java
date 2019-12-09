package repcrecdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

enum LockType
{ 
    READ, WRITE;
}

class LockEntry {
    public LockType lockType;
    public String writeLockTransaction;
    public HashSet<String> readLockTransactions; // Set<TransactionName>

    /**
     * Description: initialize all fileds
     * Input: locktype, transaction name
     * Output: N/A
     */
    public LockEntry(LockType lockType, String transactionName) {
        this.lockType = lockType;
        this.readLockTransactions = new HashSet<String>();
        if (lockType == LockType.READ) {
            this.readLockTransactions.add(transactionName);
            this.writeLockTransaction = "";
        } else if (lockType == LockType.WRITE) {
            this.writeLockTransaction = transactionName;
        }
    }

    /**
     * Description: add to lock table according to lock type
     * Input: lock type, transaction name
     * Output: succeed or not
     * Side effect: add to read lock records or write lock record according to lock type
     */
    public boolean setLock(LockType lockType, String transactionName) {
        this.lockType = lockType;
        if (lockType == LockType.READ) {
            this.readLockTransactions.add(transactionName);
            this.writeLockTransaction = "";
        } else if (lockType == LockType.WRITE) {
            this.writeLockTransaction = transactionName;
        }
        return true;
    }
}

public class DataManager {
    public int siteID;
    public HashMap<Integer, Integer> dataTable;
    public HashMap<Integer, LockEntry> lockTable; // (varName, lock entry)
    public HashMap<Integer, String> pendingWriteTable; // (varName, pend write transaction name)
    public HashMap<Integer, Boolean> repVarReadableTable; // (varID, isReadable?)
    public TreeMap<Integer, HashMap<Integer, Integer>> snapshots; // (time, map(varID, val))

    /**
     * Description: initialize site’s data
     * Input: site ID
     * Output: N/A
     */
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

    /**
     * Description: check is one lock is obtainable
     * Input: transaction name, variable ID, lock type
     * Output: a hash set of transaction names if blocked, or empty set if not blocked
     */
    public HashSet<String> checkLock(String transactionName, int varID, LockType lockType) {
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
                && (pendingWriteTran == null || lockEntry.readLockTransactions.contains(transactionName)))
            // Promote read lock to write lock if there is only one read lock and
            // this read lock comes from this T
            || (lockType == LockType.WRITE
                && lockEntry != null
                && lockEntry.lockType == LockType.READ
                && lockEntry.readLockTransactions.size() == 1
                && lockEntry.readLockTransactions.contains(transactionName)
                && (pendingWriteTran == null || pendingWriteTran.equals(transactionName)));

        // Return empty block set if no blocking
        if (suc) {
            return new HashSet<>();
        }
        return genBlockTrancSet(transactionName, lockEntry, pendingWriteTran);
    }

    /**
     * Description: genearate set contains all transaction names blocking current transaction
     * Input: transaction name, lock entry, pending write transaction
     * Output: a hash set containing all transaction names blocking current transaction
     */
    private HashSet<String> genBlockTrancSet(String transactionName, LockEntry lockEntry, String pendingWriteTran) {
        HashSet<String> set = new HashSet<String>();
        if (pendingWriteTran != null) {
            set.add(pendingWriteTran);
        }

        if (lockEntry != null) {
            String writeTranc = lockEntry.writeLockTransaction;
            if (writeTranc != null && writeTranc.length() > 0 ) {
                set.add(writeTranc);
            }
    
            for (String readTranc : lockEntry.readLockTransactions) {
                if (readTranc != null & readTranc.length() > 0) {
                    set.add(readTranc);
                }
            }
        }
        return set;
    }

    /**
     * Description: acquired the required lock if possible
     * Input: transaction name, variable id, lock type
     * Output: a hash set of transaction names if blocked, or empty set if not blocked
     * Side effect: Update lock table if the required lock can be acquired
     */
    public HashSet<String> acquireLock(String transactionName, int varID, LockType lockType) {
        if (!dataTable.containsKey(varID)) {
            return new HashSet<>();
        }

        HashSet<String> blockTrancSet = checkLock(transactionName, varID, lockType);
        if (blockTrancSet.isEmpty()) {
            if (!lockTable.containsKey(varID)) {
                lockTable.put(varID, new LockEntry(lockType, transactionName));
            } else {
                LockEntry lockEntry = lockTable.get(varID);
                lockEntry.setLock(lockType, transactionName);
            }

            String pendingWriteTran = pendingWriteTable.get(varID);
            if (lockType == LockType.WRITE
                && pendingWriteTran != null
                && pendingWriteTran.equals(transactionName)) {
                pendingWriteTable.remove(varID);
            }
            return new HashSet<>();
        }
        return blockTrancSet;
    }

    /**
     * Description: set one transaction as pending write transaction
     * Input: transaction name, variable ID
     * Output: succeed or not
     * Side effect: transaction will be set as pending write transaction if succeed
     */
    public boolean setPendingWrite(String transactionName, int varID) {
        if (pendingWriteTable.get(varID) == null) {
            pendingWriteTable.put(varID, transactionName);

            // Remove read lock from the same Transaction(blocks it from reading again)
            // as it should read the new value afterwards.
            // Otherwise, it will read the old value.
            LockEntry lockEntry = lockTable.get(varID);
            if (lockEntry != null
                && lockEntry.lockType == LockType.READ
                && lockEntry.readLockTransactions.contains(transactionName))
            {
                lockEntry.readLockTransactions.remove(transactionName);
                if (lockEntry.readLockTransactions.size() == 0) {
                    lockTable.remove(varID);
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Description: release locks obtained by one transaction
     * Input: transaction name
     * Output: void
     * Side effect: 
     * Release locks obtained by current transaction
     * Release read lock from lock table if no transaction holding read lock to one variable
     * Release write lock from lock table if it is a write lock
     */
    public void releaseLocks(String transactionName) {
        ArrayList<Integer> releasedVarIDs = new ArrayList<Integer>();
        for (Entry<Integer, LockEntry> entry: this.lockTable.entrySet()) {
            if (entry.getValue().writeLockTransaction.equals(transactionName)
                || entry.getValue().readLockTransactions.contains(transactionName))
            {
                releasedVarIDs.add(entry.getKey());
            }
        }
        for (Integer varID: releasedVarIDs) {
            LockEntry lockEntry = this.lockTable.get(varID);
            if (lockEntry.lockType == LockType.READ) {
                // Reduce read lock count and remove this lock if it comes to zero
                lockEntry.readLockTransactions.remove(transactionName);
                if (lockEntry.readLockTransactions.size() == 0) {
                    this.lockTable.remove(varID);
                }
            } else {
                // If it is a write lock, remove it as we will only have one write lock at a time
                this.lockTable.remove(varID);
            }
        }
    }

    /**
     * Description: Mimic the situation that the given site is down
     * Input: void
     * Output: void
     * Side effect: Erase the lock table and pending write table
     */
    public void fail() {
        lockTable.clear();
        pendingWriteTable.clear();
    }

    /**
     * Description: Mimic the situation that the given site is recover from down
     * Input: void
     * Output: void
     * Side effect: Set all replicated variables as non-readable(write-only)
     */
    public void recover() {
        // Set all replicated variables as non-readable(write-only)
        for (Integer varId : repVarReadableTable.keySet()) {
            repVarReadableTable.put(varId, false);
        }
    }

    /**
     * Description: Handle transaction’s read instruction
     * Input: variable ID, transaction type, transaction begin time
     * Output:
     * an integer as variable value
     * null if 
     *      site is down 
     *      variable is not available to read for read-write transaction 
     *      no corresponding snapshot for read-only transaction
     */
    public Integer read(String transactionName, int varID) {
        LockEntry lockEntry = lockTable.get(varID);
        Integer val = null;
        if (lockEntry != null
            && lockEntry.readLockTransactions.contains(transactionName)
            && lockEntry.lockType == LockType.READ)
        {
            val = dataTable.get(varID);
        }
        return val;
    }

    /**
     * Description: Handle transaction’s readRO instruction
     * Input: variable ID, transaction begin time
     * Output:
     * an integer as variable value
     * null if no avaiable snapshot
     */
    public Integer readRO(int varID, int transBeginTime) {
        HashMap<Integer, Integer> snapshot = snapshots.get(transBeginTime);
        if (snapshot == null) return null;
        return snapshot.get(varID);
    }

    /**
     * Description: Handle transaction’s write instruction
     * Input: transaction name, variable ID, variable new value
     * Output:
     *      true if write completes successfully
     *      false if not succeed or site is down
     * Side effect:
     *      Change the value of current variable to new value 
     *      If current variable is non-readable, set it to readable
     */
    public boolean write(String transactionName, int varID, int val) {
        LockEntry lockEntry = lockTable.get(varID);
        if (lockEntry != null
            && lockEntry.writeLockTransaction.equals(transactionName)
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

    /**
     * Description: take a snapshot
     * Input: current time
     * Output: void
     * Side effect: take a snapshot of current state and store it in snapshots map
     */
    public void takeSnapshot(int currentTime) {
        HashMap<Integer, Integer> snapshot = new HashMap<Integer, Integer>();
        snapshot.putAll(dataTable);
        snapshots.put(currentTime, snapshot);
    }

    /**
     * Description: returns current state of current DM (site)
     * Input: N/A
     * Output: a string contains current state
     */
    public String queryState() {
        return this.toString();
    }

    /**
     * Description: returns a string contains current state of current DM (site)
     * Input: N/A
     * Output: a string contains current state
     */
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
