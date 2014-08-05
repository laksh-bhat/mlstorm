package bolt.ml.state.weka.cluster.query;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

import java.util.Map;
import java.util.TreeMap;

/* license text */

public class VotingConsensusAggregator implements ReducerAggregator<Map<Integer, Integer>> {
    @Override
    public Map<Integer, Integer> init() {
        // A map sorted by votes
        return new TreeMap<Integer, Integer>();
    }

    @Override
    public Map<Integer, Integer> reduce(Map<Integer, Integer> reducedResult, TridentTuple tuple) {
        Map.Entry<Integer, double[]> clusterResult = (Map.Entry<Integer, double[]>) tuple.getValueByField("result");
        int partition = tuple.getIntegerByField(FieldTemplate.FieldConstants.PARTITION);

        if (reducedResult.containsValue(clusterResult.getKey())) {
            reducedResult.put(reducedResult.get(clusterResult.getKey()) + 1, clusterResult.getKey());
        } else {
            reducedResult.put(1, clusterResult.getKey());
        }

        return reducedResult;
    }
}
