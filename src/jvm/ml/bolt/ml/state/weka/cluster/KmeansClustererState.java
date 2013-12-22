package bolt.ml.state.weka.cluster;

import bolt.ml.state.weka.BaseState;
import bolt.ml.state.weka.utils.WekaUtils;
import weka.clusterers.SimpleKMeans;
import weka.core.Instance;
import weka.core.Instances;

import java.util.Collection;

/**
 * Created by lbhat@DaMSl on 12/22/13.
 * <p/>
 * Copyright {2013} {Lakshmisha Bhat}
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


/**
 * Example of a Kmeans clustering state
 * <p/>
 * Look at abstract base class for method details
 * The base class gives the structure and the ClassifierState classes implement them
 */

public class KmeansClustererState extends BaseState {
    private SimpleKMeans clusterer;
    private int numClusters;


    public KmeansClustererState (int numClusters, int windowSize) {
        super(windowSize);
        // This is where you create your own classifier and set the necessary parameters
        clusterer = new SimpleKMeans();
        this.numClusters = numClusters;
    }

    @Override
    public void train() throws Exception {
        this.clusterer.buildClusterer(dataset);
    }

    @Override
    protected void postUpdate() {}

    @Override
    protected synchronized void createDataSet() throws Exception {
        // Our aim is to create a singleton dataset which will be reused by all trainingInstances
        if(dataset != null) return;

        // hack to obtain the feature set length
        Collection<double[]> features = this.featureVectorsInWindow.values();
        for (double[] some : features) {
            loadWekaAttributes(some);
            break;
        }

        // we are now ready to create a training dataset metadata
        dataset = new Instances("training", this.wekaAttributes, windowSize);
    }

    @Override
    public int predict(Instance testInstance) throws Exception {
        assert (testInstance != null);
        return clusterer.clusterInstance(testInstance);
    }

    @Override
    protected synchronized void loadWekaAttributes(final double[] features) {
        if (this.wekaAttributes == null) {
            this.wekaAttributes = WekaUtils.getFeatureVectorForKmeansClustering(numClusters, features.length);
            this.wekaAttributes.trimToSize();
        }
    }

    public int getNumClusters() {
        return numClusters;
    }
}

