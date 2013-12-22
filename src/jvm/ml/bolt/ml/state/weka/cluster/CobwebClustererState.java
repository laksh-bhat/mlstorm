package bolt.ml.state.weka.cluster;

import bolt.ml.state.weka.BaseOnlineWekaState;
import bolt.ml.state.weka.utils.WekaUtils;
import weka.clusterers.Cobweb;
import weka.core.Instance;
import weka.core.Instances;

import java.util.Collection;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/16/13
 * Time: 7:45 PM
 */

/**
 * Example of a clustering state
 * <p/>
 * Look at abstract base class for method details
 * The base class gives the structure and the ClassifierState classes implement them
 */

public class CobwebClustererState extends BaseOnlineWekaState {
    private Cobweb clusterer;
    private int numClusters;
    private final Object lock = new Object();

    public CobwebClustererState(int numClusters, int windowSize) {
        super(windowSize);
        // This is where you create your own classifier and set the necessary parameters
        clusterer = new Cobweb();
        this.numClusters = numClusters;
    }

    @Override
    public void train(Instances trainingInstances) throws Exception {
        while (trainingInstances.enumerateInstances().hasMoreElements()) {
            train((Instance) trainingInstances.enumerateInstances().nextElement());
        }
    }

    @Override
    protected void postUpdate() {
        this.clusterer.updateFinished();
    }

    @Override
    protected synchronized void preUpdate() throws Exception {
        // Our aim is to create a singleton dataset which will be reused by all trainingInstances
        if (dataset != null) return;

        // hack to obtain the feature set length
        Collection<double[]> features = this.featureVectorsInWindow.values();
        for (double[] some : features) {
            loadWekaAttributes(some);
            break;
        }

        // we are now ready to create a training dataset metadata
        dataset = new Instances("training", this.wekaAttributes, 0);
        this.clusterer.buildClusterer(dataset.stringFreeStructure());
    }

    @Override
    public int predict(Instance testInstance) throws Exception {
        assert (testInstance != null);
        synchronized (lock) {
            return clusterer.clusterInstance(testInstance);
        }
    }

    @Override
    protected synchronized void loadWekaAttributes(final double[] features) {
        if (this.wekaAttributes == null) {
            this.wekaAttributes = WekaUtils.getFeatureVectorForOnlineClustering(numClusters, features.length);
            this.wekaAttributes.trimToSize();
        }
    }

    @Override
    protected void train(Instance instance) throws Exception {
        // setting dataset for the instance is crucial, otherwise you'll hit NPE when weka tries to create a single instance dataset internally
        instance.setDataset(dataset);
        synchronized (lock) {
            if (instance != null) this.clusterer.updateClusterer(instance);
        }
    }

    public int getNumClusters() {
        return numClusters;
    }
}
