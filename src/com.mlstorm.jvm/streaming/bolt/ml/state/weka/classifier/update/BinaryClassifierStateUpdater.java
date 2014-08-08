package bolt.ml.state.weka.classifier.update;

import backtype.storm.tuple.Values;
import bolt.ml.state.weka.MlStormWekaState;
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

/**
 * Updates binary classifier state
 */
public class BinaryClassifierStateUpdater implements StateUpdater<MlStormWekaState> {
    private final FieldTemplate fieldTemplate;
    private int localPartition, numPartitions;

    public BinaryClassifierStateUpdater(FieldTemplate template) {
        this.fieldTemplate = template;
    }

    public static class BinaryMetaClassifierStateUpdater implements StateUpdater<MlStormWekaState> {

        private final FieldTemplate fieldTemplate;
        private int localPartition, numPartitions;

        public BinaryMetaClassifierStateUpdater(FieldTemplate template) {
            this.fieldTemplate = template;
        }

        @Override
        public void updateState(final MlStormWekaState state,
                                final List<TridentTuple> tuples,
                                final TridentCollector collector) {
            for (TridentTuple tuple : tuples) {
                final Pair<Object, double[]> keyValue = FeatureVectorUtils.getKeyValuePairFromMlStormFeatureVector(fieldTemplate, tuple);
                final int key = (Integer) keyValue.getKey();
                final double[] fv = keyValue.getValue();

                state.getFeatureVectorsInCurrentWindow().put((Integer) keyValue.getKey(), keyValue.getValue());
                if ((Integer) keyValue.getKey() > state.getWindowSize() && key % state.getWindowSize() == 0) {
                    state.commit(-1L);
                }
                try {
                    collector.emit(new Values(localPartition, key, (int) state.predict(FeatureVectorUtils.buildWekaInstance(fv)), fv[fv.length - 1]));
                } catch (Exception e) {
                    if (e.toString().contains(MlStormWekaState.NOT_READY_TO_PREDICT)) {
                        // todo fix bug
                        state.commit((long) -1);
                        collector.emit(new Values(localPartition, key, (int) fv[fv.length - 1], fv[fv.length - 1]));
                    } else {
                        throw new IllegalStateException(e);
                    }
                }
            }
            Logger.getAnonymousLogger().log(Level.INFO, MessageFormat.format("finished updating state at partition [{0}] of [{1}]", localPartition, numPartitions));
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

    @Override
    public void updateState(final MlStormWekaState state,
                            final List<TridentTuple> tuples,
                            final TridentCollector collector) {
        for (TridentTuple tuple : tuples) {
            final Pair<Object, double[]> keyValue = FeatureVectorUtils.getKeyValuePairFromMlStormFeatureVector(fieldTemplate, tuple);
            final int key = (Integer) keyValue.getKey();
            final double[] fv = keyValue.getValue();

            state.getFeatureVectorsInCurrentWindow().put(key, fv);
            try {
                collector.emit(new Values(localPartition, key, (int) state.predict(FeatureVectorUtils.buildWekaInstance(fv)), fv[fv.length - 1]));
            } catch (Exception e) {
                if (e.toString().contains(MlStormWekaState.NOT_READY_TO_PREDICT)) {
                    // todo bug: fix this
                    state.commit((long) key);
                    collector.emit(new Values(localPartition, key, (int) fv[fv.length - 1], fv[fv.length - 1]));
                } else {
                    throw new IllegalStateException(MessageFormat.format("Error while updating state at partition [{0}] of [{1}]", localPartition, numPartitions), e);
                }
            }
        }
        Logger.getAnonymousLogger().log(Level.INFO, MessageFormat.format("finished updating state at partition [{0}] of [{1}]", localPartition, numPartitions));
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
