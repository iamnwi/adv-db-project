package repcrecdb;

import java.util.ArrayList;

public class TransactionManager {
    ArrayList<DataManager> dms = new ArrayList<DataManager>();

    public TransactionManager() {
        for (int i = 1; i <= 10; i++) {
            dms.add(new DataManager(i));
        }
    }

    public void run() {}

    public void parse() {}

    public void begin() {}

    public void beginRO() {}

    public void read() {}

    public void write() {}

    public void dump() {
        for (DataManager dm : dms) {
            System.out.print(dm.toString());
        }
    }

    public void end() {}

    public void fail() {}

    public void recover() {}

    public void queryState() {
        System.out.println("Transaction Manager");
        System.out.println(this.toString());
        System.out.println("Data Managers");
        for (DataManager dm : dms) {
            System.out.println(dm.toString());
        }
    }

    public void detectDeadLock() {}

    public String toString() {
        return "";
    }
}