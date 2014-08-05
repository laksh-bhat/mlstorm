package bolt.ml.state.weka.base;

import bolt.ml.state.weka.MlStormWekaState;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/* license text */

public abstract class BaseOnlineWekaState implements MlStormWekaState {

    protected final Map<Integer, double[]> featureVectorsInCurrentWindow;
    protected final long windowSize;
    protected ArrayList<Attribute> wekaAttributes = null;
    protected Instances dataset;

    /**
     * Construct the State representation for any weka based online learning algorithm
     *
     * @param windowSize the size of the sliding window (cache size)
     */
    public BaseOnlineWekaState(final int windowSize) {
        this.windowSize = windowSize;
        featureVectorsInCurrentWindow = new LinkedHashMap<Integer, double[]>(windowSize, 0.75f /*load factor*/, false) {
            public boolean removeEldestEntry(Map.Entry<Integer, double[]> eldest) {
                return size() > windowSize;
            }
        };
    }

    /**
     * do any post processing you want after updating the model
     */
    protected abstract void postUpdate();

    /**
     * do any pre update filtering/processing you want before updating the model
     *
     * @throws Exception
     */
    protected abstract void preUpdate() throws Exception;

    /**
     * @param attributeCount total number of training attributes count including class attributes
     */
    protected abstract void lazyLoadWekaAttributes(final int attributeCount);

    /**
     * @param trainingInstance
     * @throws Exception
     */
    protected abstract void train(final Instance trainingInstance) throws Exception;

    protected abstract void train(Instances trainingInstances) throws Exception;

    /**
     * Do any DB setup (or start a transaction) etc work here before you commit
     *
     * @param txId
     */
    @Override
    public void beginCommit(final Long txId) {
    }

    /**
     * This is where you do online state commit
     * In our case we train the examples and update the model to incorporate the latest batch
     *
     * @param txId
     */
    @Override
    public synchronized void commit(final Long txId) {
        // Although this looks like a windowed learning, it isn't. This is online learning! how?
        //
        final Collection<double[]> groundValues = getFeatureVectorsInCurrentWindow().values();
        try {
            preUpdate();
            updateModel(groundValues);
            postUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            persistOrCleanup(groundValues);
        }
    }

    public void persistOrCleanup(Collection<double[]> groundValues) {
        // Since we are doing online learning, we don't want to keep trained samples in memory
        // However, if you were doing windowed learning, then leave this alone i.e. don't clear the groundValues.
        //TODO persist model in database with txId as key
        groundValues.clear();
    }

    public final void updateModel(Collection<double[]> groundValues) throws Exception {
        for (double[] features : groundValues) {
            final Instance trainingInstance = new DenseInstance(wekaAttributes.size());
            for (int i = 0; i < features.length && i < wekaAttributes.size(); i++) {
                trainingInstance.setValue(i /*(Attribute) wekaAttributes.elementAt(i)*/, features[i]);
            }
            train(trainingInstance);
        }
    }

    /**
     * return the feature collection of the most recent sliding window
     */

    @Override
    public Map<Integer, double[]> getFeatureVectorsInCurrentWindow() {
        return featureVectorsInCurrentWindow;
    }
}
