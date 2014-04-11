package bolt.ml.state.weka.classifier.query;

import backtype.storm.tuple.Values;
import bolt.ml.state.weka.classifier.BinaryClassifierState;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.QueryFunction;
import storm.trident.tuple.TridentTuple;
import weka.core.Instance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by lbhat@DaMSl on 3/24/14.
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
public class BinaryClassifierQuery implements QueryFunction<BinaryClassifierState, Double> {
    private int localPartition, numPartitions;

    @Override
    public List<Double> batchRetrieve(final BinaryClassifierState binaryClassifierState, final List<TridentTuple> queryTuples) {
        List<Double> queryResults = new ArrayList<Double>();
        for (TridentTuple queryTuple : queryTuples) {
            double[] fv = getFeatureVectorFromArgs(queryTuple);
            Instance instance = binaryClassifierState.makeWekaInstance(fv);
            try {
                queryResults.add(binaryClassifierState.predict(instance));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return queryResults;
    }

    @Override
    public void execute(final TridentTuple tuple, final Double result, final TridentCollector collector) {
        collector.emit(new Values(localPartition, result));
    }

    @Override
    public void prepare(final Map map, final TridentOperationContext tridentOperationContext) {
        localPartition = tridentOperationContext.getPartitionIndex();
        numPartitions = tridentOperationContext.numPartitions();
    }

    @Override
    public void cleanup() {
    }

    private double[] getFeatureVectorFromArgs(TridentTuple queryTuple) {
        String args = queryTuple.getStringByField("args");
        String[] features = args.split(",");
        double[] fv = new double[features.length];
        for (int i = 0; i < features.length; i++) {
            String feature = features[i];
            fv[i] = Double.valueOf(feature);
        }
        return fv;
    }
}
