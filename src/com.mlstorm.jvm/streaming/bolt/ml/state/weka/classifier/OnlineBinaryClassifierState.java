package bolt.ml.state.weka.classifier;

/**
 * Created by lbhat@DaMSl on 4/17/14.
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

import bolt.ml.state.weka.base.BaseOnlineWekaState;
import bolt.ml.state.weka.utils.WekaUtils;
import weka.classifiers.Classifier;
import weka.classifiers.UpdateableClassifier;
import weka.core.Instance;
import weka.core.Instances;

import java.util.Collection;

/**
 * Example of a online learner state
 * <p/>
 * Look at abstract base class for method details
 * The base class gives the structure and the OnlineBinaryClassifierState classes implement them
 */

public class OnlineBinaryClassifierState extends BaseOnlineWekaState {

    private final Classifier updateableClassifier;
    private final Object lock = new Object();
    private boolean isTrained;

    public OnlineBinaryClassifierState(String classifier, int windowSize, String[] options) throws Exception {
        super(windowSize);
        // This is where you create your own classifier and set the necessary parameters
        updateableClassifier = WekaUtils.makeOnlineClassifier(classifier, options);
    }

    private boolean isTrained() {
        return isTrained;
    }

    @Override
    public void train(final Instances trainingInstances) throws Exception {
        while (trainingInstances.enumerateInstances().hasMoreElements()) {
            train((Instance) trainingInstances.enumerateInstances().nextElement());
        }
    }

    @Override
    protected void postUpdate() {
    }

    @Override
    protected final synchronized void preUpdate() throws Exception {
        // Our aim is to create a singleton dataset which will be reused by all trainingInstances
        if (dataset != null) {
            return;
        }

        // hack to obtain the feature set length
        Collection<double[]> features = this.featureVectorsInCurrentWindow.values();
        for (double[] some : features) {
            lazyLoadWekaAttributes(some.length);
            break;
        }

        // we are now ready to create a training dataset metadata
        dataset = new Instances("training", this.wekaAttributes, 0);
        dataset.setClassIndex(this.wekaAttributes.size() - 1);
        this.updateableClassifier.buildClassifier(dataset.stringFreeStructure());
    }

    @Override
    public long getWindowSize() {
        return super.windowSize;
    }

    @Override
    public double predict(Instance testInstance) throws Exception {
        assert (testInstance != null);
        if (!isTrained()) {
            throw new Exception(NOT_READY_TO_PREDICT);
        }

        final Instances dataUnlabeled = new Instances("TestInstances", wekaAttributes, 0 /* capacity */);
        dataUnlabeled.setClassIndex(wekaAttributes.size() - 1);
        dataUnlabeled.add(testInstance);

        synchronized (lock) {
            return (int) updateableClassifier.classifyInstance(dataUnlabeled.firstInstance());
        }
    }

    @Override
    protected synchronized void lazyLoadWekaAttributes(final int attributeCount) {
        if (this.wekaAttributes == null) {
            // Binary classification
            this.wekaAttributes = WekaUtils.makeFeatureVectorForBinaryClassification(attributeCount - 1 /* don't count class attributes */);
            this.wekaAttributes.trimToSize();
        }
    }

    @Override
    protected void train(Instance instance) throws Exception {
        // setting dataset for the instance is crucial,
        // otherwise you'll hit NPE when weka tries to create a single instance dataset internally
        instance.setDataset(dataset);
        if (instance != null && this.updateableClassifier instanceof UpdateableClassifier) {
            synchronized (lock) {
                ((UpdateableClassifier) this.updateableClassifier).updateClassifier(instance);
            }
        }
        isTrained = true;
    }
}
