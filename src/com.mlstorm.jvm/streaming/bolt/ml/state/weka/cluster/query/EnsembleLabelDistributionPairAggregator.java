package bolt.ml.state.weka.cluster.query;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;
import utils.fields.FieldTemplate;

import java.util.HashMap;
import java.util.Map;

/* license text */
public class EnsembleLabelDistributionPairAggregator implements ReducerAggregator<HashMap<Integer, Map.Entry<Integer, double[]>>> {
    @Override
    public HashMap<Integer, Map.Entry<Integer, double[]>> init() {
        // A map sorted by votes
        return new HashMap<Integer, Map.Entry<Integer, double[]>>();
    }

    @Override
    public HashMap<Integer, Map.Entry<Integer, double[]>> reduce(HashMap<Integer, Map.Entry<Integer, double[]>> reducedResult, TridentTuple tuple) {
        final int partition = tuple.getIntegerByField(FieldTemplate.FieldConstants.PARTITION);
        final Map.Entry<Integer, double[]> clusterResult = (Map.Entry<Integer, double[]>) tuple.getValueByField("result");

        if (!reducedResult.containsKey(partition)) {
            reducedResult.put(partition, clusterResult);
        }

        return reducedResult;
    }
}
