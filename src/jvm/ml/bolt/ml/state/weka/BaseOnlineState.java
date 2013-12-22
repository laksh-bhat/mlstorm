package bolt.ml.state.weka;

import storm.trident.state.State;
import weka.core.*;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/16/13
 * Time: 8:00 PM
 */

public abstract class BaseOnlineState implements State {

    /**
     * Construct the State representation for any weka based online learning algorithm
     * @param windowSize the size of the sliding window (cache size)
     */
    public BaseOnlineState (final int windowSize) {
        featureVectorsInWindow = new LinkedHashMap<Integer, double[]>(windowSize, 0.75f /*load factor*/, false) {
            public boolean removeEldestEntry (Map.Entry<Integer, double[]> eldest) {
                return size() > windowSize;
            }
        };
    }

    /**
     * Do any DB setup etc work here before you commit
     *
     * @param txId
     */
    @Override
    public void beginCommit (final Long txId) {
        // todo: prepare store for persistence here
     }

    /**
     * This is where you do online state commit
     * In our case we train the examples and update the model to incorporate the latest batch
     * @param txId
     */
    @Override
    public synchronized void commit (final Long txId) {
        // Although this looks like a windowed learning, it isn't. This is online learning
        Collection<double[]> groundValues = getFeatureVectorsInWindow().values();
        try {
            preUpdates();
            for (double[] features : groundValues) {
                loadWekaAttributes(features);
                Instance trainingInstance = new SparseInstance(features.length);
                for (int i = 0; i < features.length; i++)
                    trainingInstance.setValue((Attribute) wekaAttributes.elementAt(i), features[i]);
                train(trainingInstance);
            }
            postUpdates();
        } catch ( Exception e ) {
            e.printStackTrace();
        } finally {
            // Since we are doing online learning, we don't want to keep trained samples in memory
            // However, if you were doing windowed learning, then leave this alone i.e. don't clear the groundValues.
            //TODO persist model in database with txId as key
            groundValues.clear();
        }
    }

    protected abstract void postUpdates();

    protected abstract void preUpdates();

    /**
     * return the feature collection of the most recent window
     */

    public Map<Integer, double[]> getFeatureVectorsInWindow() {
        return featureVectorsInWindow;
    }

    /**
     * Predict the class label for the test instance
     * The input parameter is a Weka Instance without the class label
     * @param testInstance
     * @return int, as in the cluster no.
     */
    public abstract int predict(final Instance testInstance) throws Exception;

    /**
     *
     * @param features
     */
    protected abstract void loadWekaAttributes (final double[] features);

    /**
     *
     * @param trainingInstance
     * @throws Exception
     */
    protected abstract void train (final Instance trainingInstance) throws Exception;

    protected abstract void train (Instances trainingInstances) throws Exception;

    protected Map<Integer, double[]> featureVectorsInWindow;
    protected   FastVector wekaAttributes;
}
