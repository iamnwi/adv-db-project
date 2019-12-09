package repcrecdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

class WriteRecord {
    int varID;
    int value;
    HashSet<Integer> siteIDs;

    WriteRecord(int _varID, int _value) {
        this.varID = _varID;
        this.value = _value;
        this.siteIDs = new HashSet<Integer>();
    }
}

public class Transaction {
    String name;
    boolean isReadOnly;
    Integer beginTime;
    HashMap<Integer, Integer> accessedSites; // (siteID, accessTime)
    ArrayList<WriteRecord> writes;
    Integer blockedInstrCnt;

    /**
     * Description: initialize non-read-only transaction
     * Input: transaction name, begin time
     * Output: N/A
     */
    public Transaction(String name, Integer ticks) {
        this(name, ticks, false);
    }

    /**
     * Description: initialize all fields
     * Input: transaction name, begin time, is read only or not
     * Output: new transaction
     */
    public Transaction(String name, Integer ticks, boolean isReadOnly) {
        this.name = name;
        this.isReadOnly = isReadOnly;
        beginTime = ticks;
        accessedSites = new HashMap<Integer, Integer>();
        writes = new ArrayList<WriteRecord>();
        blockedInstrCnt = 0;
    }

    /**
     * Description: read from local write table
     * Input: variable id
     * Output: variable value if exists, null if not
     */
    public Integer read(int varID) {
        for (int i = this.writes.size() - 1; i >= 0; i--) {
            WriteRecord writeRec = this.writes.get(i);
            if (writeRec.varID == varID) {
                return writeRec.value;
            }
        }
        return null;
    }
}