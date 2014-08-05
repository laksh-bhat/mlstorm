package bolt.ml.state.weka.cluster.update;

/* license text */

import bolt.ml.state.weka.cluster.KmeansClustererState;
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

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/17/13
 * Time: 5:08 PM
 */
public class KmeansClusterUpdater implements StateUpdater<KmeansClustererState> {
    private final FieldTemplate template;
    private int localPartition, numPartitions;

    public KmeansClusterUpdater(FieldTemplate template) {
        this.template = template;
    }

    @Override
    public void updateState(final KmeansClustererState state,
                            final List<TridentTuple> tuples,
                            final TridentCollector collector) {
        for (TridentTuple tuple : tuples) {
            int key = tuple.getIntegerByField(template.getKeyField());
            double[] fv = (double[]) tuple.getValueByField(template.getFeatureVectorField());

            state.getFeatureVectorsInCurrentWindow().put(key, fv);
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

