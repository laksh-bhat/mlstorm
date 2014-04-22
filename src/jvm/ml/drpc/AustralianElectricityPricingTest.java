package drpc;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import bolt.ml.state.weka.cluster.query.MlStormClustererQuery;
import com.google.gson.Gson;
import org.apache.thrift7.TException;

import java.io.IOException;
import java.util.*;

import static utils.FeatureVectorUtils.serializeFeatureVector;


/**
 * Created by lbhat@DaMSl on 4/17/14.
 * <p/>
 * Copyright {2013} {Lakshmisha Bhat <laksh85@gmail.com>}
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class AustralianElectricityPricingTest {

    public static void main(final String[] args) throws IOException, TException, DRPCExecutionException {
        if (args.length < 2) {
            System.err.println("Where are the arguments? args -- HoldoutDataFile DrpcServer DrpcFunctionName");
            return;
        }

        int correct = 0, total = 0;

        final DRPCClient client = new DRPCClient(args[1], 3772, 10000 /*timeout*/);
        for (Map.Entry<Integer, double[]> features : generateHoldOutDataset(args[0])){
            int label = features.getKey();
            double[] fv = features.getValue();
            final String parameters = serializeFeatureVector(fv);
            String result = runQuery(args[1], parameters, client);
            Gson gson = new Gson();
            Object[] deserialized = gson.fromJson(result, Object[].class);
            for (Object obj : deserialized) {
                // Storm always returns a list
                List l = ((List) obj);
                int yHat = (Integer) l.get(0);
                if (yHat == label){
                    correct++;
                    System.out.println(Arrays.toString(fv) + "CORRECT");
                } else System.out.println(Arrays.toString(fv) + "INCORRECT");
                total++;
            }
        }

        System.err.println(correct/total * 1.0 + " percent correct");
        client.close();
    }

    private static String runQuery(final String topologyAndDrpcServiceName, final String args, final DRPCClient client) throws
            TException,
            DRPCExecutionException {
        return client.execute(topologyAndDrpcServiceName, args);
    }

    public static List<Map.Entry<Integer, double[]>> generateHoldOutDataset(String filename) {
        Scanner scanner = new Scanner(filename);
        List<Map.Entry<Integer, double[]>> returnList = new ArrayList<Map.Entry<Integer, double[]>>();
        int totalTests = 0;

        while (totalTests < 100 && scanner.hasNextLine()) {
            if (totalTests % 100 == 0) {
                String line = scanner.nextLine();
                String[] features = line.split(",");
                double[] fv = new double[features.length - 1];
                int label = 0;
                for (int i = 0; i < fv.length; i++) {
                    if (!features[i].equalsIgnoreCase("UP") && !features[i].equalsIgnoreCase("DOWN")) {
                        fv[i] = Double.valueOf(features[i]);
                    } else label = features[i].equalsIgnoreCase("UP")? 1 : 0;
                }
                returnList.add(new MlStormClustererQuery.Pair<Integer, double[]>(label, fv));
            }
        }
        return returnList;
    }
}
