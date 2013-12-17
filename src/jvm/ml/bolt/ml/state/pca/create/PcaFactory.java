package bolt.ml.state.pca.create;


import backtype.storm.task.IMetricsContext;
import bolt.ml.state.pca.PrincipalComponents;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 12/13/13
 * Time: 9:29 PM
 */
public class PcaFactory implements StateFactory {
    final int numSamples, sampleSize;
    PrincipalComponents pc = null;

    public PcaFactory (int numSamples, int sampleSize) {
        this.sampleSize = sampleSize;
        this.numSamples = numSamples;
    }

    @Override
    public synchronized State makeState (final Map conf,
                            final IMetricsContext metrics,
                            final int partitionIndex,
                            final int numPartitions)
    {

        if (pc == null) pc = new PrincipalComponents(numSamples, sampleSize, partitionIndex, numPartitions);
        return pc;
    }
}
