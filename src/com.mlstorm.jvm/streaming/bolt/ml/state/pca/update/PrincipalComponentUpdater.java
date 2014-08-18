/**
 * Created by lbhat@DaMSl on 1/9/14.
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
package bolt.ml.state.pca.update;

import bolt.ml.state.pca.PrincipalComponentsBase;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import utils.FeatureVectorUtils;
import utils.Pair;
import utils.fields.FieldTemplate;

import java.util.List;
import java.util.Map;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/13/13
 * Time: 10:55 PM
 */

public class PrincipalComponentUpdater implements StateUpdater<PrincipalComponentsBase> {
    private final FieldTemplate template    ;
    int localPartition, numPartitions;

    public PrincipalComponentUpdater(FieldTemplate template){
        this.template = template;
    }
    @Override
    public void updateState(final PrincipalComponentsBase state,
                            final List<TridentTuple> tuples,
                            final TridentCollector collector) {
        for (TridentTuple tuple : tuples) {
            final Pair<Object, double[]> pair = FeatureVectorUtils.getKeyValuePairFromMlStormFeatureVector(template, tuple);
            final String sensor = (String) pair.getKey();
            final double temperature = pair.getValue()[0];
            final Map<String, Double> sensors = state.getCurrentSensors();
            if (sensors.containsKey(sensor)) {
                final double existingValue = sensors.get(sensor);
                sensors.put(sensor, (existingValue + temperature) / 2.0);
            } else {
                sensors.put(sensor, temperature);
            }
        }
    }

    @Override
    public void prepare(final Map conf, final TridentOperationContext context) {
        localPartition = context.getPartitionIndex() + 1;
        numPartitions = context.numPartitions();
    }

    @Override
    public void cleanup() {
    }
}
