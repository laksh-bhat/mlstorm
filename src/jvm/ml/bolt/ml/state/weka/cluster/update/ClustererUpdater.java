package bolt.ml.state.weka.cluster.update;

/**
 * Created by lbhat@DaMSl on 12/22/13.
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

import backtype.storm.tuple.Values;
import bolt.ml.state.weka.cluster.ClustererState;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

public class ClustererUpdater implements StateUpdater<ClustererState> {

    private int localPartition, numPartitions;

    @Override
    public void updateState(final ClustererState state,
                            final List<TridentTuple> tuples,
                            final TridentCollector collector) {
        for (TridentTuple tuple : tuples) {
            double[] fv = (double[]) tuple.getValueByField("featureVectorField");
            int key = tuple.getIntegerByField("keyField");
            state.getFeatureVectorsInWindow().put(key, fv);

            try {
                if (state.isTrained()) {
                    int label = state.getClusterer().clusterInstance(state.makeWekaInstance(fv));
                    if (state.isEmitAfterUpdate()) collector.emit(new Values(localPartition, key, label));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.err.println(MessageFormat.format(
                "updating clusterer state at partition [{0}] of [{1}]", localPartition, numPartitions));
    }

    @Override
    public void prepare(final Map map, final TridentOperationContext tridentOperationContext) {
        localPartition = tridentOperationContext.getPartitionIndex();
        numPartitions = tridentOperationContext.numPartitions();
    }

    @Override
    public void cleanup() {

    }
}

