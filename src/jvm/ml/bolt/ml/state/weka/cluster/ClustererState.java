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

public class ClustererState extends BaseOnlineState {
    private Cobweb     clusterer;
    private Instances  dataInstances;
    private int        numClusters;


    public ClustererState (int numClusters, int windowSize) {
        super(windowSize);
        clusterer = new Cobweb();
        this.numClusters = numClusters;
    }

    public void train (Instances instances) throws Exception {
        while (instances.enumerateInstances().hasMoreElements()) {
            train((Instance) instances.enumerateInstances().nextElement());
        }
    }

    @Override
    protected void loadWekaAttributes (final double[] features) {
        this.attributes = WekaUtils.getFeatureVectorForClustering(numClusters, features.length);
    }

    public void train (Instance instance) throws Exception {
        clusterer.updateClusterer(instance);
    }
    public int getNumClusters () {
        return numClusters;
    }
}
