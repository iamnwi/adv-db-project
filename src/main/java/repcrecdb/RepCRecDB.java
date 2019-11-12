package repcrecdb;

import java.util.HashMap;
import java.io.FileInputStream;
import java.io.InputStream;

public class RepCRecDB {

    public static TransactionManager init() {
        HashMap<Integer, DataManager> dms = new HashMap<Integer, DataManager>();
        for (int i = 1; i <= 10; i++) {
            dms.put(i, new DataManager(i));
        }
        return new TransactionManager(dms);
    }

    public static void main(String[] args) throws Exception {
        TransactionManager tm = init();

        // Get input stream, from file or standard input
        InputStream is = null;
        if (args.length == 1) {
            String filePath = args[0];
            is = new FileInputStream(filePath);
        }
        else {
            is = System.in;
        }
        tm.run(is);
    }
}