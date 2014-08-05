package bolt.ml.state.weka.cluster.update;

import backtype.storm.tuple.Values;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/* license text */

public class MetaFeatureVectorBuilder implements Aggregator<Map<Integer, Double>> {
    @Override
    public Map<Integer, Double> init(Object batchId, TridentCollector collector) {
        return new HashMap<Integer, Double>();
    }

    @Override
    public void aggregate(Map<Integer, Double> val, TridentTuple tuple, TridentCollector collector) {
        int partition = tuple.getIntegerByField(FieldTemplate.FieldConstants.PARTITION);
        double label = tuple.getIntegerByField("label");
        val.put(partition, label);
    }

    @Override
    public void complete(Map<Integer, Double> val, TridentCollector collector) {
        double[] fv = new double[val.size()];
        for (Integer i : val.keySet()) {
            if (i < fv.length) {
                double v = val.get(i);
                fv[i] = v;
            }
        }
        collector.emit(new Values(fv));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
