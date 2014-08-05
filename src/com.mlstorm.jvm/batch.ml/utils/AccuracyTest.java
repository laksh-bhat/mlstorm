package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class AccuracyTest {
    static String algorithm;

    public static void main(String[] args) throws IOException, InterruptedException {
        algorithm = "decision_tree";
        AccuracyTest fw = new AccuracyTest();
        System.out.println("-------------------------Training------------------------");
        fw.train("E:\\code\\MachineLearning\\ML-CS475\\res");
        System.out.println("-------------------------Testing------------------------");
        fw.test("E:\\code\\MachineLearning\\ML-CS475\\res");
    }

    public void train(String path) throws IOException, InterruptedException {

        File root = new File(path);
        File[] list = root.listFiles();
        for (File f : list) {
            if (f.isDirectory()) {
                train(f.getAbsolutePath());
            } else {
                if (f.toString().endsWith(".train")) {
                    String fileName = f.toString().substring(36);

                    System.out.println(fileName);
                    //System.exit(0);
                    String dir = path.substring(36);
                    String arguments = "-algorithm " + algorithm + " -mode train -model_file "
                            + path + dir + "." + algorithm + ".model -predictions_file " + path + dir + ".predictions -data "
                            + f.toString() + " -max_decision_tree_depth 4";
                    System.out.println(arguments);
                    Runtime run = Runtime.getRuntime();
                    String cmd = "java -cp " +
                            ":E:\\code\\MachineLearning\\ML-CS475\\lib\\cli\\commons-cli-1.2\\commons-cli-1.2.jar Classify "
                            + arguments;
                    Process pr = run.exec(cmd);
                    pr.waitFor();
                    BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
                    String line;
                    while ((line = buf.readLine()) != null) {
                        System.out.println(line);
                    }
                    System.out.println("----------------------------------");
                }
            }
        }
    }

    public void test(String path) throws IOException, InterruptedException {

        File root = new File(path);
        File[] list = root.listFiles();
        for (File f : list) {
            if (f.isDirectory()) {
                test(f.getAbsolutePath());
            } else {
                if (f.toString().endsWith(".dev")) {
                    String fileName = f.toString().substring(36);
                    System.out.println(fileName);
                    //System.exit(0);
                    String dir = path.substring(36);
                    String arguments = "-algorithm " + algorithm + " -mode train -model_file res/" + dir + "/" + dir + "." + algorithm + ".model -predictions_file res/" + dir + "/" + dir + ".predictions -data " + f.toString() + " -max_decision_tree_depth 4";
                    Runtime run = Runtime.getRuntime();
                    String cmd = "java -cp bin/:../commons-cli-1.2/commons-cli-1.2.jar Classify " + arguments;
                    Process pr = run.exec(cmd);
                    pr.waitFor();
                    BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
                    String line;
                    while ((line = buf.readLine()) != null) {
                        System.out.println(line);
                    }
                    System.out.println("----------------------------------");
                }
            }
        }
    }
}
