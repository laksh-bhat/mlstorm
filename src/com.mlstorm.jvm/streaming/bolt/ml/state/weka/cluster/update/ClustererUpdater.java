package bolt.ml.state.weka.cluster.update;

 /*
 * Copyright 2013-2015 Lakshmisha Bhat
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import backtype.storm.tuple.Values;
import bolt.ml.state.weka.cluster.ClustererState;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import utils.FeatureVectorUtils;
import utils.Pair;
import utils.fields.FieldTemplate;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClustererUpdater implements StateUpdater<ClustererState> {

    private final FieldTemplate template;
    private int localPartition, numPartitions;

    public ClustererUpdater(final FieldTemplate template) {
        this.template = template;
    }

    @Override
    public void updateState(final ClustererState state,
                            final List<TridentTuple> tuples,
                            final TridentCollector collector) {
        for (TridentTuple tuple : tuples) {
            final Pair<Object, double[]> keyValue = FeatureVectorUtils.getKeyValuePairFromMlStormFeatureVector(template, tuple);
            final int key = (Integer) keyValue.getKey();
            final double[] fv = keyValue.getValue();

            state.getFeatureVectorsInCurrentWindow().put(key, fv);

            try {
                if (state.isTrainedAtLeastOnce()) {
                    int label = state.getClusterer().clusterInstance(FeatureVectorUtils.buildWekaInstance(fv));
                    if (state.isEmitAfterUpdate()) {
                        collector.emit(new Values(localPartition, key, label));
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException("Could not update clusterer state", e);
            }
        }
        Logger.getAnonymousLogger().log(Level.INFO, MessageFormat.format("updating clusterer state at partition [{0}] of [{1}]", localPartition, numPartitions));
    }

    @Override
    public void prepare(final Map map, final TridentOperationContext tridentOperationContext) {
        localPartition = tridentOperationContext.getPartitionIndex() + 1;
        numPartitions = tridentOperationContext.numPartitions();
    }

    @Override
    public void cleanup() {

    }
}

