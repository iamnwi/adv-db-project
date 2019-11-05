package repcrecdb;

import java.util.ArrayList;

public class RepCRecDB {

    public static TransactionManager init() {
        ArrayList<DataManager> dms = new ArrayList<DataManager>();
        for (int i = 1; i <= 10; i++) {
            dms.add(new DataManager(i));
        }
        return new TransactionManager(dms);
    }

    public static void main(String[] args) throws Exception {
        TransactionManager tm = init();
        tm.queryState();
        tm.dump();
    }
}