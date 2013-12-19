package bolt.ml.state.ipca.create;


import backtype.storm.task.IMetricsContext;
import bolt.ml.state.ipca.PrincipalComponents;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 12/13/13
 * Time: 9:29 PM
 */
public class PcaFactory implements StateFactory {
    final int sampleSize;
    PrincipalComponents pc = null;

    public PcaFactory (int sampleSize) {
        this.sampleSize = sampleSize;
    }

    @Override
    public synchronized State makeState (final Map conf,
                            final IMetricsContext metrics,
                            final int partitionIndex,
                            final int numPartitions)
    {

        if (pc == null) pc = new PrincipalComponents(sampleSize, partitionIndex, numPartitions);
        return pc;
    }
}
