package bolt.ml.state.weka.cluster;

import bolt.ml.state.weka.BaseState;
import bolt.ml.state.weka.utils.WekaUtils;
import weka.clusterers.Cobweb;
import weka.core.*;

import java.util.Collection;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/16/13
 * Time: 7:45 PM
 */

public class ClustererState extends BaseState {
    private Cobweb     clusterer;
    private Instances  dataInstances;
    private int        numClusters;
    private FastVector attributes;

    public ClustererState (int numClusters) {
        clusterer = new Cobweb();
        this.numClusters = numClusters;
    }

    public void train (Instances instances) throws Exception {
        while (instances.enumerateInstances().hasMoreElements()) {
            train((Instance) instances.enumerateInstances().nextElement());
        }
    }

    public void train (Instance instance) throws Exception {
        clusterer.updateClusterer(instance);
    }


    @Override

    public void beginCommit (final Long txId) {}

    @Override
    public void commit (final Long txId) {
        Collection<Double[]> groundValues = getFeatures().asMap().values();
        try {
            for (Double[] features : groundValues) {
                getWekaAttributes(features);
                Instance trainingInstance = new SparseInstance(features.length);
                for (int i = 0; i < features.length; i++)
                    trainingInstance.setValue((Attribute) attributes.elementAt(i), features[i]);
                train(trainingInstance);
            }
        } catch ( Exception e ) {
            e.printStackTrace();
        } finally {
            groundValues.clear();
            getFeatures().invalidateAll();
            //TODO persist clusterer in database with txId
        }
    }

    private void getWekaAttributes (final Double[] features) {
        if (this.attributes != null)
            this.attributes = WekaUtils.getFeatureVectorForClustering(numClusters, features.length);
    }

    public int getNumClusters () {
        return numClusters;
    }
}
