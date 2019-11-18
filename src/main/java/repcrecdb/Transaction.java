package repcrecdb;

import java.util.HashMap;
import java.util.HashSet;

public class Transaction {
    String name;
    boolean isReadOnly;
    Integer beginTime;
    HashSet<Integer> accessedSites;
    HashMap<Integer, Integer> writes; // (varName, newVal)
    Integer blockedInstrCnt;

    public Transaction(String name, Integer ticks) {
        this(name, ticks, false);
    }

    public Transaction(String name, Integer ticks, boolean isReadOnly) {
        this.name = name;
        this.isReadOnly = isReadOnly;
        beginTime = ticks;
        accessedSites = new HashSet<Integer>();
        writes = new HashMap<Integer, Integer>();
        blockedInstrCnt = 0;
    }

    public Integer read(int varID) {
        return this.writes.get(varID);
    }
}