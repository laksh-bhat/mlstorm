package bolt.ml.state.weka.cluster.create;

import backtype.storm.task.IMetricsContext;
import bolt.ml.state.weka.cluster.ClustererState;
import bolt.ml.state.weka.cluster.CobwebClustererState;
import bolt.ml.state.weka.cluster.KmeansClustererState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import utils.fields.FieldTemplate;

import java.util.Map;

/* license text */

public class MlStormClustererFactory {

    /**
     * Cluster state factory for any generic clusterer
     */
    public static class ClustererFactory implements StateFactory {
        private final int windowSize, k;
        private final String clustererName;
        private final String[] options;
        private final boolean emitTuples;
        private final FieldTemplate template;
        private ClustererState state = null;

        public ClustererFactory(int k, int windowSize, String clustererName, boolean emitTuplesAfterUpdate, FieldTemplate template, String[] options) {
            this.k = k;
            this.windowSize = windowSize;
            this.clustererName = clustererName;
            this.emitTuples = emitTuplesAfterUpdate;
            this.options = options;
            this.template = template;
        }

        @Override
        public State makeState(final Map map,
                               final IMetricsContext iMetricsContext,
                               final int partitionIndex,
                               final int numPartitions) {
            if (state == null) {
                try {
                    state = new ClustererState(clustererName, k, windowSize, template, emitTuples, options);
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to construct Clusterer state", e);
                }
            }
            return state;
        }
    }

    /**
     * Cluster state factory for Cobweb clusterer
     */
    public static class CobwebClustererFactory implements StateFactory {
        private final int windowSize, k;
        private CobwebClustererState state = null;

        public CobwebClustererFactory(int k, int windowSize) {
            this.k = k;
            this.windowSize = windowSize;
        }

        @Override
        public State makeState(final Map map,
                               final IMetricsContext iMetricsContext,
                               final int partitionIndex,
                               final int numPartitions) {
            if (state == null) {
                state = new CobwebClustererState(k, windowSize);
            }
            return state;
        }
    }

    /**
     * Cluster state factory for K-means clusterer
     */
    public static class KmeansClustererFactory implements StateFactory {
        private final int windowSize, k;
        private final FieldTemplate fieldTemplate;
        private KmeansClustererState state = null;

        public KmeansClustererFactory(int k, int windowSize, FieldTemplate fieldTemplate) {
            this.k = k;
            this.windowSize = windowSize;
            this.fieldTemplate = fieldTemplate;
        }

        @Override
        public State makeState(final Map map,
                               final IMetricsContext iMetricsContext,
                               final int partitionIndex,
                               final int numPartitions) {
            if (state == null) {
                try {
                    state = new KmeansClustererState(k, windowSize, fieldTemplate);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return state;
        }
    }
}
