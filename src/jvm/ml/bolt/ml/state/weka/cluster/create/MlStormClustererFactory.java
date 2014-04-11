package bolt.ml.state.weka.cluster.create;

import backtype.storm.task.IMetricsContext;
import bolt.ml.state.weka.cluster.ClustererState;
import bolt.ml.state.weka.cluster.CobwebClustererState;
import bolt.ml.state.weka.cluster.KmeansClustererState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;
/**
 * Created by lbhat@DaMSl on 4/10/14.
 * <p/>
 * Copyright {2013} {Lakshmisha Bhat <laksh85@gmail.com>}
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class MlStormClustererFactory {

    /**
     * Cluster state factory for any generic clusterer
     */
    public static class ClustererFactory implements StateFactory {
        private final int windowSize, k;
        private final String clustererName;
        private ClustererState state = null;
        private final boolean emitTuples;

        public ClustererFactory(int k, int windowSize, String clustererName, boolean emitTuplesAfterUpdate) {
            this.k = k;
            this.windowSize = windowSize;
            this.clustererName = clustererName;
            this.emitTuples = emitTuplesAfterUpdate;
        }

        @Override
        public State makeState (final Map map,
                                final IMetricsContext iMetricsContext,
                                final int partitionIndex,
                                final int numPartitions)
        {
            if (state == null) try {
                state = new ClustererState(clustererName, k, windowSize, emitTuples);
            } catch (Exception e) {
                System.err.println("Could not ");
                e.printStackTrace();
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

    /**
     * Cluster state factory for K-means clusterer
     */
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
