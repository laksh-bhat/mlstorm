package bolt.ml.state.weka.classifier.update;

import backtype.storm.tuple.Values;
import bolt.ml.state.weka.cluster.query.MlStormClustererQuery;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

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

public class MetaFeatureVectorBuilder implements Aggregator<Map<Integer, Map.Entry<Double, Double>>> {
    @Override
    public Map<Integer,  Map.Entry<Double, Double>> init(Object batchId, TridentCollector collector) {
        return new HashMap<Integer,  Map.Entry<Double, Double>>();
    }

    @Override
    public void aggregate(Map<Integer, Map.Entry<Double, Double>> val, TridentTuple tuple, TridentCollector collector) {
        int partition = tuple.getIntegerByField("partition");
        double label = tuple.getIntegerByField("label");
        int actualLabel = tuple.getIntegerByField("actualLabel");
        val.put(partition, new MlStormClustererQuery.Pair<Double, Double>(label, (double) actualLabel));
    }

    @Override
    public void complete(Map<Integer, Map.Entry<Double, Double>> val, TridentCollector collector) {
        double[] fv = new double[val.size() + 1]; // make room for actualLabel from training data-set.
        double actualLabel = 0;
        for (Integer i : val.keySet()) {
            if (i < fv.length){
                double v = val.get(i).getKey();
                actualLabel = val.get(i).getValue();
                fv[i] = v;
            }
        }
        fv[fv.length - 1] = actualLabel;

        collector.emit(new Values(fv));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
