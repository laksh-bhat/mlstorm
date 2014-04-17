package bolt.ml.state.weka.classifier.update;

import backtype.storm.tuple.Values;
import bolt.ml.state.weka.MlStormWekaState;
import org.apache.commons.lang.ArrayUtils;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import topology.weka.EnsembleLearnerTopologyBase;

import java.text.MessageFormat;
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
public class BinaryClassifierStateUpdater implements StateUpdater<MlStormWekaState> {

    private int localPartition, numPartitions;

    @Override
    public void updateState (final MlStormWekaState state,
                             final List<TridentTuple> tuples,
                             final TridentCollector collector)
    {
        for (TridentTuple tuple : tuples) {
            Double[] fv = (Double[]) tuple.getValueByField(EnsembleLearnerTopologyBase.featureVector.get(0));
            state.getFeatureVectorsInWindow().put(tuple.getIntegerByField(EnsembleLearnerTopologyBase.key.get(0)), ArrayUtils.toPrimitive(fv));
            try {
                if (state.isTrained())
                    collector.emit(new Values(localPartition, (int) state.predict(state.makeWekaInstance(ArrayUtils.toPrimitive(fv)))));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        System.err.println(MessageFormat.format(
                "finished updating state at partition [{0}] of [{1}]", localPartition, numPartitions));
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
