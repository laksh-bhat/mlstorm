package bolt.ml.state.weka;

/**
 * Created by lbhat@DaMSl on 12/22/13.
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

import storm.trident.state.State;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

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

public abstract class BaseWekaState implements State {

    /**
     * Construct the State representation for any weka based learning algorithm
     *
     * @param windowSize the size of the sliding window (cache size)
     */
    public BaseWekaState(final int windowSize) {
        this.windowSize = windowSize;
        featureVectorsInWindow = new LinkedHashMap<Integer, double[]>(windowSize, 0.75f /*load factor*/, false) {
            public boolean removeEldestEntry(Map.Entry<Integer, double[]> eldest) {
                return size() > windowSize;
            }
        };
    }

    /**
     * return the feature collection of the most recent window
     */

    public Map<Integer, double[]> getFeatureVectorsInWindow() {
        return featureVectorsInWindow;
    }

    /**
     * Predict the class label for the test instance
     * The input parameter is a Weka Instance without the class label
     *
     *
     *
     * @param testInstance
     * @return double, as in the cluster/classification no.
     */
    public abstract double predict(final Instance testInstance) throws Exception;

    /**
     * returns average training time
     * @return
     */
    public long getTrainingDuration() {
        return trainingDuration;
    }

    /**
     * Do any DB setup etc work here before you commit
     *
     * @param txId
     */
    @Override
    public void beginCommit(final Long txId) {}

    /**
     * This is where you do state commit
     * In our case we train the examples and create the model to for this sliding window
     *
     * @param txId
     */
    @Override
    public synchronized void commit(final Long txId) {
        // this is windowed learning.
        Collection<double[]> groundValues = getFeatureVectorsInWindow().values();
        try {
            createDataSet();
            for (double[] features : groundValues) {
                Instance trainingInstance = new DenseInstance(wekaAttributes.size());
                for (int i = 0; i < features.length && i < wekaAttributes.size(); i++)
                    trainingInstance.setValue(i , features[i]);
                dataset.add(trainingInstance);
            }
            train();
            postUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            dataset.delete();
        }
    }

    public Instance makeWekaInstance(double[] featureVector){
        if (wekaAttributes == null) loadWekaAttributes(featureVector);
        
        Instance instance = new DenseInstance(wekaAttributes.size());
        for (int i = 0; i < featureVector.length && i < wekaAttributes.size(); i++)
            instance.setValue(i , featureVector[i]);
        return instance;
    }

    /**
     * do anything you want after updating the classifier
     */
    protected abstract void postUpdate();
    protected abstract void emptyDataset();

    /**
     * do anything you want after updating the classifier
     * @throws Exception
     */
    protected abstract void createDataSet() throws Exception;

    /**
     * @param features a double array representing at least one feature vector.
     * you must load weka attributes in the createDataSet method to
     * guarantee that you have all the details necessary to create a dataset.
     */
    protected abstract void loadWekaAttributes(final double[] features);

    /**
     * @throws Exception
     * Train the classifier/clusterer on the above generated dataset.
     */
    protected abstract void train() throws Exception;

    protected Map<Integer, double[]> featureVectorsInWindow;
    protected ArrayList<Attribute> wekaAttributes;
    protected Instances dataset;
    protected final int windowSize;
    protected long trainingDuration;
}

