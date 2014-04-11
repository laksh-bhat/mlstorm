package bolt.ml.state.weka.classifier;

import bolt.ml.state.weka.BaseWekaState;
import weka.classifiers.Classifier;
import weka.classifiers.meta.RandomCommittee;
import weka.core.Instance;

/**
 * Created by lbhat@DaMSl on 3/4/14.
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
public class MetaClassifierState extends BaseWekaState {
    private Classifier classifier;
    /**
     * Construct the State representation for any weka based learning algorithm
     *
     * @param windowSize the size of the sliding window (cache size)
     */
    public MetaClassifierState(int windowSize) {
        super(windowSize);
        classifier = new RandomCommittee();

    }

    @Override
    public double predict(Instance testInstance) throws Exception {
        return 0;
    }

    @Override
    protected void postUpdate() {

    }

    @Override
    protected void emptyDataset() {

    }

    @Override
    protected void createDataSet() throws Exception {

    }

    @Override
    protected void loadWekaAttributes(double[] features) {

    }

    @Override
    protected void train() throws Exception {

    }
}
