package bolt.ml.state.weka.cluster;

import bolt.ml.state.weka.BaseWekaState;
import bolt.ml.state.weka.utils.WekaUtils;
import weka.clusterers.SimpleKMeans;
import weka.core.Instance;
import weka.core.Instances;

import java.util.Collection;

/**
 * Created by lbhat@DaMSl on 12/22/13.
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


/**
 * Kmeans clustering state abstraction
 * <p/>
 * Look at abstract base class for method details
 * The base class gives the structure (abstract methods) and the <Classifier>State classes implement them
 */

public class KmeansClustererState extends BaseWekaState {
    private SimpleKMeans clusterer;
    private int numClusters;
    private int featuresCount;
    private final Object lock;
    private boolean isTrained;


    public KmeansClustererState(int numClusters, int windowSize) throws Exception {
        super(windowSize);
        // This is where you create your own classifier and set the necessary parameters
        clusterer = new SimpleKMeans();
        clusterer.setNumClusters(numClusters);
        this.numClusters = numClusters;
        lock = new Object();
    }

    /**
     * I trust you.
     * Call this very when you don't have ANY other choice and when you aren't updating the clusterer
     *
     * @return
     */
    public SimpleKMeans getClusterer() {
        return clusterer;
    }

    public int getNumClusters() {
        return numClusters;
    }

    @Override
    protected void emptyDataset() {
        synchronized (lock) {
            this.dataset.clear();
        }
    }

    @Override
    public boolean isTrained() {
        return isTrained;
    }

    @Override
    public double predict(Instance testInstance) throws Exception {
        assert (testInstance != null);
        synchronized (lock) {
            return clusterer.clusterInstance(testInstance);
        }
    }

    /**
     * Allows parameter updates to the current model
     *
     * @param k
     * @throws Exception
     */
    public final void updateClustererNumClusters(int k) throws Exception {
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        System.err.println("DEBUG: updating k and rebuilding clusterer");
        synchronized (lock) {
            numClusters = k;
            clusterer = new SimpleKMeans();
            clusterer.setNumClusters(numClusters);
            this.wekaAttributes = WekaUtils.makeFeatureVectorForBatchClustering(numClusters, featuresCount);
            this.wekaAttributes.trimToSize();
            this.dataset = new Instances("training", this.wekaAttributes, this.windowSize);
            this.trainingDuration = 0;
        }
    }

    @Override
    protected void train() throws Exception {
        long startTime = System.currentTimeMillis();
        synchronized (lock) {
            this.clusterer.buildClusterer(dataset);
        }
        long endTime = System.currentTimeMillis();
        this.trainingDuration = (getTrainingDuration() + (endTime - startTime)) / 2;
        isTrained = true;
    }

    @Override
    protected void postUpdate() {
    }

    @Override
    protected synchronized void createDataSet() throws Exception {
        synchronized (lock) {
            // Our aim is to create a singleton dataset which will be reused by all trainingInstances
            if (this.dataset != null) return;

            // hack to obtain the feature set length
            Collection<double[]> features = this.featureVectorsInWindow.values();
            for (double[] some : features) {
                loadWekaAttributes(some);
                break;
            }

            // we are now ready to create a training dataset metadata
            dataset = new Instances("training", this.wekaAttributes, this.windowSize);
        }
    }

    @Override
    protected synchronized void loadWekaAttributes(final double[] features) {
        if (this.wekaAttributes == null) {
            this.featuresCount = features.length;
            this.wekaAttributes = WekaUtils.makeFeatureVectorForBatchClustering(numClusters, features.length);
            this.wekaAttributes.trimToSize();
        }
    }
}

