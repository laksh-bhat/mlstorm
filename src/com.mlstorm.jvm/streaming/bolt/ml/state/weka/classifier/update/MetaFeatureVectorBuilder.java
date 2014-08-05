package bolt.ml.state.weka.classifier.update;

import backtype.storm.tuple.Values;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import utils.Pair;
import utils.fields.FieldTemplate;

import java.util.HashMap;
import java.util.Map;

/* license text */

public class MetaFeatureVectorBuilder implements Aggregator<Map<Integer, Map.Entry<Double, Double>>> {
    @Override
    public Map<Integer, Map.Entry<Double, Double>> init(Object batchId, TridentCollector collector) {
        return new HashMap<Integer, Map.Entry<Double, Double>>();
    }

    @Override
    public void aggregate(Map<Integer, Map.Entry<Double, Double>> val, TridentTuple tuple, TridentCollector collector) {
        final int partition = tuple.getIntegerByField(FieldTemplate.FieldConstants.PARTITION);
        final double label = tuple.getIntegerByField(FieldTemplate.FieldConstants.CLASSIFICATION.LABEL);
        final double actualLabel = tuple.getDoubleByField(FieldTemplate.FieldConstants.CLASSIFICATION.ACTUAL_LABEL);
        val.put(partition, new Pair<Double, Double>(label, actualLabel));
    }

    @Override
    public void complete(Map<Integer, Map.Entry<Double, Double>> val, TridentCollector collector) {
        double[] fv = new double[val.size() + 1]; // make room for actualLabel from training data-set.
        double actualLabel = 0;
        for (Integer i : val.keySet()) {
            if (i < fv.length) {
                final double v = val.get(i).getKey();
                actualLabel = val.get(i).getValue();
                fv[i] = v;
            }
        }
        fv[fv.length - 1] = actualLabel;
        collector.emit(new Values(fv));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
