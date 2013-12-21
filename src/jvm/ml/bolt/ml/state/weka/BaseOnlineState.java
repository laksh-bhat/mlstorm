package bolt.ml.state.weka;

import storm.trident.state.State;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.SparseInstance;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/16/13
 * Time: 8:00 PM
 */
public abstract class BaseOnlineState implements State {
    public BaseOnlineState (final int window) {
        features = new LinkedHashMap<Integer, double[]>(100, 0.75f, false) {
            public boolean removeEldestEntry (Map.Entry<Integer, double[]> eldest) {
                return size() > window;
            }
        };
    }

    @Override
    public void beginCommit (final Long txId) {}

    @Override
    public synchronized void commit (final Long txId) {
        // Although this looks like a windowed learning, it isn't. This is online learning
        Collection<double[]> groundValues = getFeatures().values();
        try {
            for (double[] features : groundValues) {
                loadWekaAttributes(features);
                Instance trainingInstance = new SparseInstance(features.length);
                for (int i = 0; i < features.length; i++)
                    trainingInstance.setValue((Attribute) attributes.elementAt(i), features[i]);
                train(trainingInstance);
            }
        } catch ( Exception e ) {
            e.printStackTrace();
        } finally {
            // Since we are doing online learning, we don't want to keep trained samples in memory
            // However, if you were doing windowed learning, then leave this alone i.e. don't clear the groundValues.
            //TODO persist model in database with txId as key
            groundValues.clear();
        }
    }

    protected abstract void loadWekaAttributes (final double[] features);

    protected abstract void train (final Instance trainingInstance) throws Exception;

    public Map<Integer, double[]> getFeatures () {
        return features;
    }

    protected Map<Integer, double[]> features;
    protected   FastVector             attributes;
}
