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
    private int        featureWidth;
    private int        numClusters;
    private FastVector attributes;

    public ClustererState (int featureWidth, int numClusters) {
        clusterer = new Cobweb();
        this.featureWidth = featureWidth;
        this.numClusters = numClusters;
        this.attributes = WekaUtils.getFeatureVectorForClustering(numClusters, featureWidth);
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
        Instance trainingInstance = null;
        try {
            for (Double[] feature : groundValues) {
                trainingInstance = new SparseInstance(featureWidth);
                for (int i = 0; i < featureWidth; i++)
                    trainingInstance.setValue((Attribute) attributes.elementAt(i), feature[i]);
            }
            train(trainingInstance);
        } catch ( Exception e ) {
            e.printStackTrace();
        } finally {
            groundValues.clear();
            getFeatures().invalidateAll();
            //TODO persist clusterer in database with txId
        }
    }
}
