package bolt.ml.state.weka.cluster.create;

import backtype.storm.task.IMetricsContext;
import bolt.ml.state.weka.cluster.CobwebClustererState;
import bolt.ml.state.weka.cluster.KmeansClustererState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/17/13
 * Time: 5:10 PM
 */

public class ClustererFactory{
    public static class CobwebClustererFactory implements StateFactory {
        private final int windowSize, k;
        private CobwebClustererState state = null;

        public CobwebClustererFactory (int k, int windowSize) {
            this.k = k;
            this.windowSize = windowSize;
        }

        @Override
        public State makeState (final Map map,
                                final IMetricsContext iMetricsContext,
                                final int partitionIndex,
                                final int numPartitions)
        {
            if (state == null) state = new CobwebClustererState(k, windowSize);
            return state;
        }
    }

    public static class KmeansClustererFactory implements StateFactory {
        private final int windowSize, k;
        private KmeansClustererState state = null;

        public KmeansClustererFactory (int k, int windowSize) {
            this.k = k;
            this.windowSize = windowSize;
        }

        @Override
        public State makeState (final Map map,
                                final IMetricsContext iMetricsContext,
                                final int partitionIndex,
                                final int numPartitions)
        {
            if (state == null) try {
                state = new KmeansClustererState(k, windowSize);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return state;
        }
    }
}
