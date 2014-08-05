package bolt.ml.state.weka.cluster;

import bolt.ml.state.weka.base.BaseWekaState;
import bolt.ml.state.weka.utils.WekaUtils;
import utils.fields.FieldTemplate;
import weka.clusterers.Clusterer;
import weka.core.Instance;
import weka.core.Instances;

import java.util.Collection;

/* license text */

/**
 * General clustering state abstraction
 * <p/>
 * Look at abstract base class for method details
 * The base class gives the structure (abstract methods) and the <Classifier/Clusterer>State classes implement them
 */

public class ClustererState extends BaseWekaState {
    private final Clusterer clusterer;
    private final int numClusters;
    private final Object lock;
    private final boolean emitAfterUpdate;
    private boolean isTrainedAtLeastOnce;


    public ClustererState(String clustererName, int numClusters, int windowSize, FieldTemplate fieldTemplate, boolean emitAfterUpdate, String[] options) throws Exception {
        super(windowSize, fieldTemplate);
        // This is where you create your own classifier and set the necessary parameters
        this.clusterer = WekaUtils.makeClusterer(clustererName, numClusters, options);
        this.numClusters = numClusters;
        this.lock = new Object();
        this.emitAfterUpdate = emitAfterUpdate;
    }

    private void updateStatistics(long startTime, long endTime) {
        this.statistics.setTrainingDuration((getStatistics().getTrainingDuration() + (endTime - startTime)) / 2);
        this.isTrainedAtLeastOnce = true;
    }

    /**
     * I trust you!
     * Call this only when you don't have ANY other choice and when you aren't updating the clusterer.
     *
     * @return
     */
    public Clusterer getClusterer() {
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
    public double predict(Instance testInstance) throws Exception {
        assert (testInstance != null);
        synchronized (lock) {
            return clusterer.clusterInstance(testInstance);
        }
    }

    @Override
    public void beginCommit(Long txId) {
        lazyLoadWekaAttributes(getFieldTemplate().getNumFeatures());
    }

    @Override
    protected void train() throws Exception {
        synchronized (lock) {
            long startTime = System.currentTimeMillis();
            this.clusterer.buildClusterer(dataset);
            long endTime = System.currentTimeMillis();
            updateStatistics(startTime, endTime);
        }
    }

    @Override
    protected void postUpdate() {
    }

    @Override
    protected synchronized void createDataSet() throws Exception {
        // Our aim is to create a singleton dataset which will be reused by all trainingInstances
        if (this.dataset != null) {
            return;
        }

        // hack to obtain the feature set length
        Collection<double[]> features = this.featureVectorsInWindow.values();
        for (double[] some : features) {
            lazyLoadWekaAttributes(some.length);
            break;
        }

        // we are now ready to create a training dataset metadata
        dataset = new Instances("clusterer-training", this.wekaAttributes, this.windowSize);
    }

    @Override
    protected synchronized void lazyLoadWekaAttributes(final int featureCount) {
        if (this.wekaAttributes == null) {
            this.wekaAttributes = WekaUtils.makeFeatureVectorForBatchClustering(featureCount, numClusters);
            this.wekaAttributes.trimToSize();
        }
    }

    public boolean isEmitAfterUpdate() {
        return emitAfterUpdate;
    }

    @Override
    public long getWindowSize() {
        return super.windowSize;
    }

    public boolean isTrainedAtLeastOnce() {
        return isTrainedAtLeastOnce;
    }
}

