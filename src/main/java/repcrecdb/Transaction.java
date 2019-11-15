package repcrecdb;

import java.util.ArrayList;
import java.util.HashMap;

public class Transaction {
    String name;
    boolean isReadOnly;
    Integer beginTime;
    ArrayList<Integer> accessedSites;
    HashMap<Integer, Integer> writes; // (varName, newVal)

    public Transaction(String name, Integer ticks) {
        this(name, ticks, false);
    }

    public Transaction(String name, Integer ticks, boolean isReadOnly) {
        this.name = name;
        this.isReadOnly = isReadOnly;
        beginTime = ticks;
        accessedSites = new ArrayList<Integer>();
        writes = new HashMap<Integer, Integer>();
    }

    public Integer read(int varID) {
        return this.writes.get(varID);
    }
}