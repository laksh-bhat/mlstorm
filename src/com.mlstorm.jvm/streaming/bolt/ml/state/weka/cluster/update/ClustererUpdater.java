package bolt.ml.state.weka.cluster.update;

/* license text */

import backtype.storm.tuple.Values;
import bolt.ml.state.weka.cluster.ClustererState;
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

public class ClustererUpdater implements StateUpdater<ClustererState> {

    private final FieldTemplate template;
    private int localPartition, numPartitions;

    public ClustererUpdater(FieldTemplate template) {
        this.template = template;
    }

    @Override
    public void updateState(final ClustererState state,
                            final List<TridentTuple> tuples,
                            final TridentCollector collector) {
        for (TridentTuple tuple : tuples) {
            final int key = tuple.getIntegerByField(template.getKeyField());
            final double[] fv = (double[]) tuple.getValueByField(template.getFeatureVectorField());
            state.getFeatureVectorsInCurrentWindow().put(key, fv);

            try {
                if (state.isTrainedAtLeastOnce()) {
                    int label = state.getClusterer().clusterInstance(FeatureVectorUtils.buildInstance(fv));
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

