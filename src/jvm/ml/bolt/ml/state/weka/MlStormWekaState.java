package bolt.ml.state.weka;

import storm.trident.state.State;
import weka.core.Instance;

import java.util.Map;

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
public interface MlStormWekaState extends State {
    final static String NOT_READY_TO_PREDICT = "NotReadyToPredict";

    Instance makeWekaInstance(double[] featureVector);

    long getWindowSize();

    boolean isTrained();

    @Override
    void beginCommit(Long txId);

    @Override
    void commit(Long txId);

    Map<Integer, double[]> getFeatureVectorsInWindow();

    /**
     * Predict the class label for the test instance
     * The input parameter is a Weka Instance without the class label
     *
     * @param testInstance
     * @return double, as in the cluster no/class no..
     */
    double predict(Instance testInstance) throws Exception;
}
