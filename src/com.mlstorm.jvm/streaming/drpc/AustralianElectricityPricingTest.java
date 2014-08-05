package drpc;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import com.google.gson.Gson;
import org.apache.thrift7.TException;
import utils.Pair;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static utils.FeatureVectorUtils.serializeFeatureVector;


/* license text */

public class AustralianElectricityPricingTest {

    public static List<Pair<Double, double[]>> generateHoldOutDataset(String filename) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(filename));
        List<Pair<Double, double[]>> returnList = new ArrayList<Pair<Double, double[]>>();
        int totalTests = 0;

        while (totalTests < 45000 && scanner.hasNextLine()) {
            if (totalTests % 25 == 0) {
                String line = scanner.nextLine();
                String[] features = line.split(",");
                double[] fv = new double[features.length - 1];
                double label = 0;
                for (int i = 0; i < fv.length; i++) {
                    if (!features[i].equalsIgnoreCase("UP") && !features[i].equalsIgnoreCase("DOWN")) {
                        fv[i] = Double.valueOf(features[i]);
                    } else {
                        label = features[i].equalsIgnoreCase("DOWN") ? 0 : 1;
                    }
                }
                returnList.add(new Pair<Double, double[]>(label, fv));
            }
        }
        return returnList;
    }

    public static void main(final String[] args) throws IOException, TException, DRPCExecutionException {
        if (args.length < 3) {
            System.err.println("Where are the arguments? args -- HoldoutDataFile DrpcServer DrpcFunctionName");
            return;
        }

        double correct = 0, total = 0;

        final DRPCClient client = new DRPCClient(args[1], 3772, 1000000 /*timeout*/);
        for (Map.Entry<Double, double[]> features : generateHoldOutDataset(args[0])) {
            double label = features.getKey();
            double[] fv = features.getValue();
            final String parameters = serializeFeatureVector(fv);
            String result = runQuery(args[2], parameters, client);
            Gson gson = new Gson();
            Object[] deserialized = gson.fromJson(result, Object[].class);
            for (Object obj : deserialized) {
                // Storm always returns a list
                List l = ((List) obj);
                double yHat = (Double) l.get(0);
                if (yHat == label) {
                    correct++;
                    System.out.println(Arrays.toString(fv) + " CORRECT");
                } else {
                    System.out.println(Arrays.toString(fv) + " INCORRECT");
                }
                total++;
            }
        }

        System.err.println(correct / total + " percent correct");
        client.close();
    }

    private static String runQuery(final String topologyAndDrpcServiceName, final String args, final DRPCClient client) throws
            TException,
            DRPCExecutionException {
        return client.execute(topologyAndDrpcServiceName, args);
    }
}
