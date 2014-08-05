package bolt.ml.state.weka.classifier.update;

import backtype.storm.tuple.Values;
import bolt.ml.state.weka.MlStormWekaState;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import utils.FeatureVectorUtils;
import utils.fields.FieldTemplate;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/* license text */

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
                final int key = tuple.getIntegerByField(fieldTemplate.getKeyField());
                final double[] fv = (double[]) tuple.getValueByField(fieldTemplate.getFeatureVectorField());

                state.getFeatureVectorsInCurrentWindow().put(key, fv);
                if (key > state.getWindowSize() && key % state.getWindowSize() == 0) {
                    state.commit(-1L);
                }
                try {
                    collector.emit(new Values(localPartition, key, (int) state.predict(FeatureVectorUtils.buildInstance(fv)), fv[fv.length - 1]));
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
            final int key = tuple.getIntegerByField(fieldTemplate.getKeyField());
            final double[] fv = (double[]) tuple.getValueByField(fieldTemplate.getFeatureVectorField());
            state.getFeatureVectorsInCurrentWindow().put(key, fv);
            try {
                collector.emit(new Values(localPartition, key, (int) state.predict(FeatureVectorUtils.buildInstance(fv)), fv[fv.length - 1]));
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
