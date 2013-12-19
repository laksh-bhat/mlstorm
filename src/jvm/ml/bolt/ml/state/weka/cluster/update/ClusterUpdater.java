package bolt.ml.state.weka.cluster.update;

import bolt.ml.state.weka.cluster.ClustererState;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/17/13
 * Time: 5:08 PM
 */
public class ClusterUpdater implements StateUpdater<ClustererState> {

    private int localPartition, numPartitions;
    private Logger logger = LogManager.getLogManager().getLogger("");

    @Override
    public void updateState (final ClustererState state,
                             final List<TridentTuple> tuples,
                             final TridentCollector collector)
    {
        for (TridentTuple tuple : tuples) {
            Double[] fv = (Double[]) tuple.getValueByField("featureVector");
            state.getFeatures().asMap().putIfAbsent(tuple.getIntegerByField("key"), fv);
        }

        logger.log(Level.ALL, MessageFormat.format(
                "Updating state at partition [{0}] of [{1}]", localPartition, numPartitions));
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
