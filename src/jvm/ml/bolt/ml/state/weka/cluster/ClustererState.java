package bolt.ml.state.weka.cluster;

import bolt.ml.state.weka.BaseOnlineState;
import bolt.ml.state.weka.utils.WekaUtils;
import weka.clusterers.Cobweb;
import weka.core.Instance;
import weka.core.Instances;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/16/13
 * Time: 7:45 PM
 */

/**
 * Example of a clustering state
 *
 * Look at abstract base class for method details
 * The base class gives the structure and the ClassifierState classes implement them
 */

public class ClustererState extends BaseOnlineState {
    private Cobweb     clusterer;
    private int        numClusters;


    public ClustererState (int numClusters, int windowSize) {
        super(windowSize);
        // This is where you create your own classifier and set the necessary parameters
        clusterer = new Cobweb();
        this.numClusters = numClusters;
    }

    @Override
    public void train (Instances trainingInstances) throws Exception {
        while (trainingInstances.enumerateInstances().hasMoreElements()) {
            train((Instance) trainingInstances.enumerateInstances().nextElement());
        }
    }

    @Override
    protected void postUpdate() {
        this.clusterer.updateFinished();
    }

    @Override
    protected void preUpdate() throws Exception {
        Instances data = new Instances("data", this.wekaAttributes, 0);
        //data.add(new Instance(this.wekaAttributes.size()));
        clusterer.buildClusterer(data);
    }

    @Override
    public int predict(Instance testInstance) throws Exception {
        return clusterer.clusterInstance(testInstance);
    }

    @Override
    protected void loadWekaAttributes (final double[] features) {
        if (this.wekaAttributes == null)
            this.wekaAttributes = WekaUtils.getFeatureVectorForClustering(numClusters, features.length);
    }

    @Override
    protected void train (Instance instance) throws Exception {
        clusterer.updateClusterer(instance);
    }

    public int getNumClusters () {
        return numClusters;
    }
}
