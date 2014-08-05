package drpc;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.commons.lang.ArrayUtils;
import org.apache.thrift7.TException;
import utils.SpoutUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static utils.FeatureVectorUtils.serializeFeatureVector;

/**
 * Created by lbhat@DaMSl on 4/10/14.
 * <p/>
 * Copyright {2013} {Lakshmisha Bhat}
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

/**
 * This is a query to ask questions about the model built on BPTI features.
 * <p/>
 * Run drpc query as
 * java -cp .:`storm classpath`:$REPO/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar drpc.BptiEnsembleQuery $DRPC_SRVER EnsembleClusterer /$BPTI_REPO/bpti_db/features
 */

public class BptiEnsembleQuery {
    public static void main(final String[] args) throws IOException, TException, DRPCExecutionException {
        if (args.length < 3) {
            System.err.println("Where are the arguments? args -- DrpcServer DrpcFunctionName folder");
            return;
        }

        final DRPCClient client = new DRPCClient(args[0], 3772, 1000000 /*timeout*/);
        final Queue<String> featureFiles = new ArrayDeque<String>();
        SpoutUtils.listFilesForFolder(new File(args[2]), featureFiles);

        Scanner scanner = new Scanner(featureFiles.peek());
        int i = 0;
        while (scanner.hasNextLine() && i++ < 1) {
            List<Map<String, List<Double>>> dict = SpoutUtils.pythonDictToJava(scanner.nextLine());
            for (Map<String, List<Double>> map : dict) {
                final Double[] features = map.get("chi1").toArray(new Double[0]);
                final Double[] moreFeatures = map.get("chi2").toArray(new Double[0]);
                final Double[] both = (Double[]) ArrayUtils.addAll(features, moreFeatures);
                final String parameters = serializeFeatureVector(ArrayUtils.toPrimitive(both));
                Logger.getAnonymousLogger().log(Level.INFO, runQuery(args[1], parameters, client));
            }
        }
        client.close();
    }

    private static String runQuery(final String topologyAndDrpcServiceName, final String args, final DRPCClient client) throws
            TException,
            DRPCExecutionException {
        return client.execute(topologyAndDrpcServiceName, args);
    }
}
