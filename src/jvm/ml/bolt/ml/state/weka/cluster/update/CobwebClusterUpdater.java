package bolt.ml.state.weka.cluster.update;

import bolt.ml.state.weka.cluster.CobwebClustererState;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;
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

public class CobwebClusterUpdater implements StateUpdater<CobwebClustererState> {

    private int localPartition, numPartitions;

    @Override
    public void updateState (final CobwebClustererState state,
                             final List<TridentTuple> tuples,
                             final TridentCollector collector)
    {
        for (TridentTuple tuple : tuples) {
            double[] fv = (double[]) tuple.getValueByField("featureVector");
            state.getFeatureVectorsInWindow().put(tuple.getIntegerByField("key"), fv);
        }

        System.err.println(MessageFormat.format(
            "updating state at partition [{0}] of [{1}]", localPartition, numPartitions));
    }

    @Override
    public void prepare (final Map map, final TridentOperationContext tridentOperationContext) {
        localPartition = tridentOperationContext.getPartitionIndex();
        numPartitions = tridentOperationContext.numPartitions();
    }

    @Override
    public void cleanup () {

    }
}
