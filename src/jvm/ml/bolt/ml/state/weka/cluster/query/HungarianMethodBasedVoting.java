package bolt.ml.state.weka.cluster.query;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import utils.HungarianAlgorithm;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lbhat@DaMSl on 4/10/14.
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
public class HungarianMethodBasedVoting implements Function {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        HashMap<Integer, Map.Entry<Integer, double[]>> voteMap =
                (HashMap<Integer, Map.Entry<Integer, double[]>>) tuple.getValueByField("voteMap");

        int k = 0;
        int partitions = voteMap.keySet().size();
        for (Integer key:voteMap.keySet()){
            double[] val = voteMap.get(key).getValue();
            k = val.length;
        }
        double[][] costMatrix = new double[k][k];
        for (int i /* partitions */ = 0; i < partitions ; i++){
            for (int j /* clusters */ = 0; j < k; j++){
                costMatrix[i][j] = 1.0 - voteMap.get(i).getValue()[j];
            }
        }
        HungarianAlgorithm hungarian = new HungarianAlgorithm(costMatrix);
        int [] partitionLabelling = hungarian.execute();

    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        System.err.println(MessageFormat.format("Aggregating based on hungarian method at {0} of {1}",
                context.getPartitionIndex(), context.numPartitions()));
    }

    @Override
    public void cleanup() {

    }
}
