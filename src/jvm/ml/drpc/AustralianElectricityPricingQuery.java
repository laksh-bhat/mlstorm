package drpc;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;

import java.io.IOException;

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
public class AustralianElectricityPricingQuery {

    public static void main(final String[] args) throws IOException, TException, DRPCExecutionException {
        if (args.length < 2) {
            System.err.println("Where are the arguments? args -- DrpcServer DrpcFunctionName");
            return;
        }

        final DRPCClient client = new DRPCClient(args[0], 3772, 1000000 /*timeout*/);

        final double[] features = {0.0,2.0,0.021277,0.051699,0.415055,0.003467,0.422915,0.414912};
        final String parameters = serializeFeatureVector(features);

        System.err.println(runQuery(args[1], parameters, client));
        client.close();
    }

    private static String runQuery(final String topologyAndDrpcServiceName, final String args, final DRPCClient client) throws
            TException,
            DRPCExecutionException {
        return client.execute(topologyAndDrpcServiceName, args);
    }
}
