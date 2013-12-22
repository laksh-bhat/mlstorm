package bolt.ml.state.weka.cluster.update;

import bolt.ml.state.weka.cluster.ClustererState;
import org.apache.commons.lang.ArrayUtils;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/17/13
 * Time: 5:08 PM
 */
public class ClusterUpdater implements StateUpdater<ClustererState> {

    private int localPartition, numPartitions;

    @Override
    public void updateState (final ClustererState state,
                             final List<TridentTuple> tuples,
                             final TridentCollector collector)
    {
        for (TridentTuple tuple : tuples) {
            Double[] fv = (Double[]) tuple.getValueByField("featureVector");
            state.getFeatureVectorsInWindow().put(tuple.getIntegerByField("key"), ArrayUtils.toPrimitive(fv));
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
