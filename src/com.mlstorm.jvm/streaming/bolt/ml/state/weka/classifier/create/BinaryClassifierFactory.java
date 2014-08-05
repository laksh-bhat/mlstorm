package bolt.ml.state.weka.classifier.create;

import backtype.storm.task.IMetricsContext;
import bolt.ml.state.weka.MlStormWekaState;
import bolt.ml.state.weka.classifier.BinaryClassifierState;
import bolt.ml.state.weka.classifier.OnlineBinaryClassifierState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import utils.fields.FieldTemplate;

import java.util.Map;

/* license text */
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
