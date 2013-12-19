package bolt.ml.state.weka.cluster.create;

import backtype.storm.task.IMetricsContext;
import bolt.ml.state.weka.cluster.ClustererState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/17/13
 * Time: 5:10 PM
 */
public class ClustererFactory implements StateFactory {
    private ClustererState state = null;
    private int k;

    public ClustererFactory (int k) {
        this.k = k;
    }

    @Override
    public State makeState (final Map map,
                            final IMetricsContext iMetricsContext,
                            final int partitionIndex,
                            final int numPartitions)
    {
        if (state == null) state = new ClustererState(k);
        return state;
    }
}
