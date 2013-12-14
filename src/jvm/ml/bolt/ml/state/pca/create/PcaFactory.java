package bolt.ml.state.pca.create;


import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 12/13/13
 * Time: 9:29 PM
 */
public class PcaFactory implements StateFactory {
    @Override
    public State makeState (final Map conf,
                            final IMetricsContext metrics,
                            final int partitionIndex,
                            final int numPartitions)
    {
        return null;
    }
}
