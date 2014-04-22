package bolt.ml.state.weka.classifier;

import bolt.ml.state.weka.BaseWekaState;
import bolt.ml.state.weka.utils.WekaUtils;
import weka.classifiers.Classifier;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

import java.util.Collection;

/**
 * Created by lbhat@DaMSl on 2/10/14.
 * <p/>
 * Copyright {2013} {Lakshmisha Bhat}
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
public class BinaryClassifierState extends BaseWekaState {

    private Classifier classifier;
    private final Object lock;
    private boolean isTrained;

    /**
     * Construct the State representation for any weka based learning algorithm
     *
     * @param windowSize the size of the sliding window (cache size)
     */
    public BinaryClassifierState(String classifier, int windowSize, String[] options) throws Exception {
        super(windowSize);
        this.classifier = WekaUtils.makeClassifier(classifier, options);
        lock = new Object();
    }

    @Override
    public boolean isTrained() {
        return isTrained;
    }

    @Override
    public double predict(Instance testInstance) throws Exception {
        assert testInstance != null;
        if (!isTrained()) throw new Exception(NOT_READY_TO_PREDICT);

        Instances datasetUnlabeled = new Instances("TestInstances", wekaAttributes, 0);
        datasetUnlabeled.setClassIndex(wekaAttributes.size() - 1);
        datasetUnlabeled.add(testInstance);

        synchronized (lock) {
            return (int) classifier.classifyInstance(datasetUnlabeled.firstInstance());
        }
    }

    @Override
    public synchronized void commit(final Long txId) {
        // this is windowed learning.
        Collection<double[]> groundValues = getFeatureVectorsInWindow().values();
        try {
            createDataSet();
            for (double[] features : groundValues) {
                Instance trainingInstance = new DenseInstance(wekaAttributes.size());
                trainingInstance.setDataset(dataset);
                for (int i = 0; i < features.length && i < wekaAttributes.size(); i++){
                    if (i != wekaAttributes.size() - 1) trainingInstance.setValue(i , features[i]);
                    else trainingInstance.setValue(dataset.attribute(WekaUtils.CLASSES_ATTR_NAME), features[i]);
                }
            }
            train();
            postUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            dataset.clear();
        }
    }

    @Override
    protected void postUpdate() {

    }

    @Override
    protected void emptyDataset() {
        dataset.clear();
    }

    @Override
    protected void createDataSet() throws Exception {
        // Our aim is to create a singleton dataset which will be reused by all trainingInstances
        if (this.dataset != null) return;

        // hack to obtain the feature set length
        Collection<double[]> features = this.featureVectorsInWindow.values();
        for (double[] some : features) {
            loadWekaAttributes(some);
            break;
        }

        // we are now ready to create a training dataset metadata
        dataset = new Instances("training", this.wekaAttributes, this.windowSize);
        dataset.setClass(dataset.attribute(WekaUtils.CLASSES_ATTR_NAME));
    }

    @Override
    protected void loadWekaAttributes(double[] features) {
        if (this.wekaAttributes == null) {
            this.wekaAttributes = WekaUtils.makeFeatureVectorForBinaryClassification(features.length);
            this.wekaAttributes.trimToSize();
        }
    }

    @Override
    protected void train() throws Exception {
        synchronized (lock) {
            long startTime = System.currentTimeMillis();
            this.classifier.buildClassifier(dataset);
            long endTime = System.currentTimeMillis();
            this.trainingDuration = (getTrainingDuration() + (endTime - startTime))/2;
        }
        isTrained = true;
    }


}
