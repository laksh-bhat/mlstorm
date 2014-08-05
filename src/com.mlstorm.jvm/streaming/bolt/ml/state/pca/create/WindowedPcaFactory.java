package bolt.ml.state.pca.create;


import backtype.storm.task.IMetricsContext;
import bolt.ml.state.pca.PrincipalComponentsBase;
import bolt.ml.state.pca.windowed.WindowedStormPca;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.sql.SQLException;
import java.util.Map;

/* license text */


public class WindowedPcaFactory implements StateFactory {
    final int sampleSize;
    private final int numPrincipalComponents;
    PrincipalComponentsBase pc = null;

    public WindowedPcaFactory(int sampleSize, int numPrincipalComponents) {
        this.sampleSize = sampleSize;
        this.numPrincipalComponents = numPrincipalComponents;
    }

    @Override
    public synchronized State makeState(final Map conf,
                                        final IMetricsContext metrics,
                                        final int partitionIndex,
                                        final int numPartitions) {
        if (pc == null) {
            try {
                pc = new WindowedStormPca(sampleSize, numPrincipalComponents, partitionIndex, numPartitions);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return pc;
    }
}
