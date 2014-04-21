package bolt.ml.state.weka.cluster;

import bolt.ml.state.weka.BaseWekaState;
import bolt.ml.state.weka.utils.WekaUtils;
import weka.clusterers.Clusterer;
import weka.core.Instance;
import weka.core.Instances;

import java.util.Collection;

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
/**
 *  General clustering state abstraction
 * <p/>
 * Look at abstract base class for method details
 * The base class gives the structure (abstract methods) and the <Classifier/Clusterer>State classes implement them
 */

public class ClustererState extends BaseWekaState {
    private final Clusterer clusterer;
    private final int numClusters;
    private final Object lock;
    private final boolean emitAfterUpdate;
    private boolean isTrained;


    public ClustererState(String clustererName, int numClusters, int windowSize, boolean emitAfterUpdate, String[] options) throws Exception {
        super(windowSize);
        // This is where you create your own classifier and set the necessary parameters
        this.clusterer = WekaUtils.makeClusterer(clustererName, numClusters, options);
        this.numClusters = numClusters;
        this.lock = new Object();
        this.emitAfterUpdate = emitAfterUpdate;
    }

    /**
     * I trust you!
     * Call this only when you don't have ANY other choice and when you aren't updating the clusterer.
     * @return
     */
    public Clusterer getClusterer(){ return clusterer; }

    public int getNumClusters() {
        return numClusters;
    }

    @Override
    protected void emptyDataset(){
        synchronized (lock) {
            this.dataset.clear();
        }
    }

    @Override
    public double predict(Instance testInstance) throws Exception {
        assert (testInstance != null);
        synchronized (lock) {
            return clusterer.clusterInstance(testInstance);
        }
    }

    @Override
    protected void train() throws Exception {
        synchronized (lock) {
            long startTime = System.currentTimeMillis();
            this.clusterer.buildClusterer(dataset);
            long endTime = System.currentTimeMillis();
            this.trainingDuration = (getTrainingDuration() + (endTime - startTime))/2;
            this.isTrained = true;
        }
    }

    @Override
    protected void postUpdate() {
    }

    @Override
    protected synchronized void createDataSet() throws Exception {
        // Our aim is to create a singleton dataset which will be reused by all trainingInstances
        if (this.dataset != null) return;

        // hack to obtain the feature set length
        Collection<double[]> features = this.featureVectorsInWindow.values();
        for (double[] some : features) {
            loadWekaAttributes(some);
            break;
        }

        // we are now ready to create a training dataset metadata
        dataset = new Instances("clusterer-training", this.wekaAttributes, this.windowSize);
    }

    @Override
    protected synchronized void loadWekaAttributes(final double[] features) {
        if (this.wekaAttributes == null) {
            this.wekaAttributes = WekaUtils.makeFeatureVectorForBatchClustering(features.length, numClusters);
            this.wekaAttributes.trimToSize();
        }
    }

    public boolean isEmitAfterUpdate() {
        return emitAfterUpdate;
    }

    public boolean isTrained() {
        return isTrained;
    }
}

