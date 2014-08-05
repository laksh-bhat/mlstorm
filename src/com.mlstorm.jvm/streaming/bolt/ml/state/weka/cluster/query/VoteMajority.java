package bolt.ml.state.weka.cluster.query;

import backtype.storm.tuple.Values;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;
import java.util.TreeMap;

/* license text */
public class VoteMajority implements Function {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        //noinspection unchecked
        TreeMap<Integer, Integer> voteMap = (TreeMap<Integer, Integer>) tuple.getValueByField("voteMap");
        collector.emit(new Values(voteMap.lastEntry().getValue()));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
