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
    
    public LockEntry(LockType lockType, String transactionName) {
        this.lockType = lockType;
        this.transactionName = transactionName;
    }
}

public class DataManager {
    public int siteIndex;
    public HashMap<Integer, Integer> dataTable;
    public int lastDownTime;
    public boolean isUp; // indicate whether this site is up or down
    public HashMap<String, LockEntry> lockTable; // (varName, (lock type, transName))

    public DataManager(int index) {
        siteIndex = index;
        isUp = true;
        lastDownTime = 0;
        lockTable = new HashMap<String, LockEntry>();
        dataTable = new HashMap<Integer, Integer>();
        for (int i = 1; i <= 20; i++) {
            if (i % 2 == 0 || 1 + (i%10) == siteIndex) {
                dataTable.put(i, 10*i);
            }
        }
    }

    public void checkLock() {}
    
    public void acquireLock() {}

    public void fail(int currentTime) {
        isUp = false;
        lastDownTime = currentTime;
        lockTable = new HashMap<String, LockEntry>();
    }

    public void recover() {}

    public void read() {}

    public void write() {}

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
        return String.format("site %d - %s", siteIndex, dataStringBuilder.toString());
    }
}