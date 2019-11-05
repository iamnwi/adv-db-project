package repcrecdb;

public class RepCRecDB {
    public static void main(String[] args) throws Exception {
        TransactionManager tm = new TransactionManager();
        tm.queryState();
        tm.dump();
    }
}