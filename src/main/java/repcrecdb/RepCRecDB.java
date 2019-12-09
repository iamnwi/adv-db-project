package repcrecdb;

import java.util.HashMap;
import java.util.Scanner;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

public class RepCRecDB {

    /**
     * Description: create new transaction manager for main program
     * Input: N/A
     * Output: return new transaction manager
     */
    public static TransactionManager init() {
        HashMap<Integer, DataManager> dms = new HashMap<Integer, DataManager>();
        for (int i = 1; i <= 10; i++) {
            dms.put(i, new DataManager(i));
        }
        return new TransactionManager(dms);
    }

    /**
     * Description: main program
     * Input: program arguments
     * Output: N/A
     * Side effect: Get input stream, from file or run all test cases
     */
    public static void main(String[] args) throws Exception {
        TransactionManager tm = init();

        // Get input stream, from file or run all test cases
        InputStream is = null;
        if (args.length == 1) {
            String filePath = args[0];
            is = new FileInputStream(filePath);
            tm.run(is);
        }
        else {
            // Run all test cases
            runIntegrationTests();
        }
    }

    /**
     * Description: run all tests
     * Input: N/A
     * Output: N/A
     * Side effect: run all tests and print out test result
     */
    private static void runIntegrationTests() {
        File[] files = new File("tests").listFiles();
        for (File file : files) {
            String filePath = file.getPath();
            if (filePath.endsWith(".in")) {
                String ansFilePath = filePath.replace(".in", ".ans");
                assert(new File(ansFilePath).exists());

                // Print content of test file
                System.out.println("--------Test Input("+filePath+")--------");
                try(InputStream is = new FileInputStream(filePath);) {
                    try(Scanner input = new Scanner(is);) {
                        while (input.hasNext()) {
                            System.out.println(input.nextLine());
                        }
                    }
                } catch (Exception e) {
                    System.out.print(e);
                }

                // Print content of answer file
                System.out.println("--------Expected Output("+ansFilePath+")--------");
                try(InputStream is = new FileInputStream(ansFilePath);) {
                    try(Scanner input = new Scanner(is);) {
                        while (input.hasNext()) {
                            System.out.println(input.nextLine());
                        }
                    }
                } catch (Exception e) {
                    System.out.print(e);
                }

                // Print content of actual output
                System.out.println("--------Actual Output--------");
                try(InputStream is = new FileInputStream(filePath);) {
                    TransactionManager tm = RepCRecDB.init();
                    tm.run(is);
                } catch (Exception e)
                {
                    System.out.println(e);
                }
                System.out.println();
            }
        }
    }
}