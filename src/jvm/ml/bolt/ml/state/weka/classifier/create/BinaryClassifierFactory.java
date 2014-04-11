package bolt.ml.state.weka.classifier.create;

import backtype.storm.task.IMetricsContext;
import bolt.ml.state.weka.classifier.BinaryClassifierState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Created by lbhat@DaMSl on 3/24/14.
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
public class BinaryClassifierFactory implements StateFactory {
    private final int windowSize;
    private final String classifier;
    private BinaryClassifierState state = null;

    public BinaryClassifierFactory (String classifier, int windowSize) {
        this.classifier = classifier;
        this.windowSize = windowSize;
    }

    @Override
    public State makeState (final Map map,
                            final IMetricsContext iMetricsContext,
                            final int partitionIndex,
                            final int numPartitions)
    {
        if (state == null) state = new BinaryClassifierState(classifier, windowSize);
        return state;
    }
}
