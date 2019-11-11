package repcrecdb;

import java.util.ArrayList;
import java.util.HashMap;

public class Transaction {
    String name;
    boolean isReadOnly;
    Integer beginTime;
    ArrayList<Integer> accessedSites;
    HashMap<String, Integer> writes; // (varName, newVal)

    public Transaction(String name, Integer ticks) {
        this(name, ticks, false);
    }

    public Transaction(String name, Integer ticks, boolean isReadOnly) {
        this.name = name;
        this.isReadOnly = isReadOnly;
        beginTime = ticks;
        accessedSites = new ArrayList<Integer>();
        writes = new HashMap<String, Integer>();
    }
}