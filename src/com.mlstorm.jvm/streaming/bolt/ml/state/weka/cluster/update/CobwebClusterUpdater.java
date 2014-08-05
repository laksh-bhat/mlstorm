package bolt.ml.state.weka.cluster.update;

import bolt.ml.state.weka.cluster.CobwebClustererState;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import utils.fields.FieldTemplate;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/* license text */

public class CobwebClusterUpdater implements StateUpdater<CobwebClustererState> {

    private final FieldTemplate template;
    private int localPartition, numPartitions;

    public CobwebClusterUpdater(FieldTemplate template) {
        this.template = template;
    }

    @Override
    public void updateState(final CobwebClustererState state,
                            final List<TridentTuple> tuples,
                            final TridentCollector collector) {
        for (TridentTuple tuple : tuples) {
            double[] fv = (double[]) tuple.getValueByField(template.getFeatureVectorField());
            state.getFeatureVectorsInCurrentWindow().put(tuple.getIntegerByField(template.getKeyField()), fv);
        }
        Logger.getAnonymousLogger().log(Level.INFO, MessageFormat.format("updating state at partition [{0}] of [{1}]", localPartition, numPartitions));
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
