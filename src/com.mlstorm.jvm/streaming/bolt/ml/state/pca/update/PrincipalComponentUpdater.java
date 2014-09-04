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
import utils.MlStormFeatureVectorUtils;
import utils.KeyValuePair;
import utils.fields.FieldTemplate;

import java.util.List;
import java.util.Map;

/**
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
            final KeyValuePair<Object, double[]> keyValuePair = MlStormFeatureVectorUtils.getKeyValueFromMlStormFeatureVector(template, tuple);
            final String sample = (String) keyValuePair.getKey();
            final double temperature = keyValuePair.getValue()[0];
            final Map<String, Double> samples = state.getCurrentSamples();
            if (samples.containsKey(sample)) {
                final double existingValue = samples.get(sample);
                samples.put(sample, (existingValue + temperature) / 2.0);
            } else {
                samples.put(sample, temperature);
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
