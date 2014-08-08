package bolt.ml.state.weka.classifier.create;

import backtype.storm.task.IMetricsContext;
import bolt.ml.state.weka.MlStormWekaState;
import bolt.ml.state.weka.classifier.BinaryClassifierState;
import bolt.ml.state.weka.classifier.OnlineBinaryClassifierState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import utils.fields.FieldTemplate;

import java.util.Map;

 /*
 * Copyright 2013-2015 Lakshmisha Bhat
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
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
    private final FieldTemplate fieldTemplate;
    private BinaryClassifierState state = null;

    public BinaryClassifierFactory(String classifier, int windowSize, FieldTemplate template, String[] options) {
        this.classifier = classifier;
        this.windowSize = windowSize;
        this.options = options;
        this.fieldTemplate = template;
    }

    public static class OnlineBinaryClassifierFactory implements StateFactory {
        private final int windowSize;
        private final String classifier;
        private final String[] options;
        private MlStormWekaState state = null;

        public OnlineBinaryClassifierFactory(String classifier, int windowSize, String[] options) {
            this.classifier = classifier;
            this.windowSize = windowSize;
            this.options = options;
        }

        @Override
        public State makeState(final Map map,
                               final IMetricsContext iMetricsContext,
                               final int partitionIndex,
                               final int numPartitions) {
            if (state == null) {
                try {
                    state = new OnlineBinaryClassifierState(classifier, windowSize, options);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            return state;
        }
    }

    @Override
    public State makeState(final Map map,
                           final IMetricsContext iMetricsContext,
                           final int partitionIndex,
                           final int numPartitions) {
        if (state == null) {
            try {
                state = new BinaryClassifierState(classifier, windowSize, fieldTemplate, options);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        return state;
    }
}
