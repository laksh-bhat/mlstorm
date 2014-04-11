package bolt.ml.state.weka.cluster.update;

import backtype.storm.tuple.Values;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
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

public class MetaFeatureVectorBuilder implements Aggregator<List<Double>> {
    @Override
    public List<Double> init(Object batchId, TridentCollector collector) {
        return new ArrayList<Double>();
    }

    @Override
    public void aggregate(List<Double> val, TridentTuple tuple, TridentCollector collector) {
        int partition = tuple.getIntegerByField("partition");
        double label = tuple.getIntegerByField("label");
        val.add(partition, label);
    }

    @Override
    public void complete(List<Double> val, TridentCollector collector) {
        double[] fv = new double[val.size()];
        for (int i = 0; i < val.size(); i++) {
            double v = val.get(i);
            fv[i] = v;
        }
        collector.emit(new Values(fv));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
