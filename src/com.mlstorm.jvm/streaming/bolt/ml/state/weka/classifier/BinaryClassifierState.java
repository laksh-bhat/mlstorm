package bolt.ml.state.weka.classifier;

import bolt.ml.state.weka.base.BaseWekaState;
import bolt.ml.state.weka.utils.WekaUtils;
import utils.fields.FieldTemplate;
import weka.classifiers.Classifier;
import weka.core.Instance;
import weka.core.Instances;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    private final Object lock;
    private Classifier classifier;
    private boolean isTrainedAtLeastOnce;

    /**
     * Construct the State representation for any weka based learning algorithm
     *
     * @param windowSize the size of the sliding window (cache size)
     */
    public BinaryClassifierState(String classifier, int windowSize, FieldTemplate template, String[] options) throws Exception {
        super(windowSize, template);
        this.classifier = WekaUtils.makeClassifier(classifier, options);
        lock = new Object();
    }

    private boolean isTrainedAtLeastOnce() {
        return isTrainedAtLeastOnce;
    }

    @Override
    public long getWindowSize() {
        return super.windowSize;
    }

    @Override
    public double predict(Instance testInstance) throws Exception {
        if (testInstance == null) {
            throw new IllegalStateException("test instance is null");
        } else if (!isTrainedAtLeastOnce()) {
            throw new Exception(NOT_READY_TO_PREDICT);
        }

        final Instances datasetUnlabeled = new Instances("TestInstances", wekaAttributes, 0);
        datasetUnlabeled.setClassIndex(wekaAttributes.size() - 1);
        datasetUnlabeled.add(testInstance);

        synchronized (lock) {
            return (int) classifier.classifyInstance(datasetUnlabeled.firstInstance());
        }
    }

    @Override
    public void beginCommit(Long txId) {

    }

    @Override
    public synchronized void commit(final Long txId) {
        Logger.getAnonymousLogger().log(Level.INFO, MessageFormat.format("{2} invoked for transaction {0} by thread {1}", txId, Thread.currentThread().getId(), "Commit"));

        // this is windowed learning.
        final Collection<double[]> groundValues = getFeatureVectorsInCurrentWindow().values();
        try {
            super.preUpdate();
            super.update(groundValues);
            postUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
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
        if (this.dataset != null) {
            return;
        }

        // hack to obtain the feature set length
        Collection<double[]> features = this.featureVectorsInWindow.values();
        for (double[] some : features) {
            lazyLoadWekaAttributes(some.length);
            break;
        }

        // we are now ready to create a training dataset metadata
        dataset = new Instances("training", this.wekaAttributes, this.windowSize);
        dataset.setClassIndex(wekaAttributes.size() - 1);
    }

    @Override
    protected void lazyLoadWekaAttributes(final int featureCount) {
        if (this.wekaAttributes == null) {
            this.wekaAttributes = WekaUtils.makeFeatureVectorForBinaryClassification(featureCount - 1); // ignore class attribute
            this.wekaAttributes.trimToSize();
        }
    }

    @Override
    protected void train() throws Exception {
        synchronized (lock) {
            final long startTime = System.currentTimeMillis();
            this.classifier.buildClassifier(dataset);
            final long endTime = System.currentTimeMillis();
            this.statistics.setTrainingDuration((getStatistics().getTrainingDuration() + (endTime - startTime)) / 2);
        }
        isTrainedAtLeastOnce = true;
    }
}
