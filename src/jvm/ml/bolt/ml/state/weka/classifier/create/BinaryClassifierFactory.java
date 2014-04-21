package bolt.ml.state.weka.classifier.create;

import backtype.storm.task.IMetricsContext;
import bolt.ml.state.weka.MlStormWekaState;
import bolt.ml.state.weka.classifier.BinaryClassifierState;
import bolt.ml.state.weka.classifier.OnlineBinaryClassifierState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import weka.Run;

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
    private final String[] options;
    private BinaryClassifierState state = null;

    public BinaryClassifierFactory (String classifier, int windowSize, String[] options) {
        this.classifier = classifier;
        this.windowSize = windowSize;
        this.options    = options;
    }

    @Override
    public State makeState (final Map map,
                            final IMetricsContext iMetricsContext,
                            final int partitionIndex,
                            final int numPartitions)
    {
        if (state == null) try {
            state = new BinaryClassifierState(classifier, windowSize, options);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return state;
    }

    public static class OnlineBinaryClassifierFactory implements StateFactory {
        private final int windowSize;
        private final String classifier;
        private final String[] options;
        private MlStormWekaState state = null;

        public OnlineBinaryClassifierFactory (String classifier, int windowSize, String[] options) {
            this.classifier = classifier;
            this.windowSize = windowSize;
            this.options = options;
        }

        @Override
        public State makeState (final Map map,
                                final IMetricsContext iMetricsContext,
                                final int partitionIndex,
                                final int numPartitions)
        {
            if (state == null) try {
                state = new OnlineBinaryClassifierState(classifier, windowSize, options);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return state;
        }
    }
}
