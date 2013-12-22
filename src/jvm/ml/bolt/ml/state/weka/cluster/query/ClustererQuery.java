package bolt.ml.state.weka.cluster.query;

import backtype.storm.tuple.Values;
import bolt.ml.state.weka.cluster.ClustererState;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.QueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/16/13
 * Time: 7:44 PM
 */
public class ClustererQuery implements QueryFunction<ClustererState, String> {
    @Override
    public List<String> batchRetrieve (final ClustererState clustererState, final List<TridentTuple> queryTuples) {
        List<String> queryResults = new ArrayList<String>();
        for (TridentTuple query : queryTuples){
            if(query.getStringByField("args").split(",")[0].trim().equals(String.valueOf(clustererState.getNumClusters()))){
                //todo: do something
            }
        }
        return queryResults;
    }

    @Override
    public void execute (final TridentTuple tuple, final String label, final TridentCollector collector) {
        collector.emit(new Values(label));
    }

    @Override
    public void prepare (final Map map, final TridentOperationContext tridentOperationContext) {}

    @Override
    public void cleanup () {}
}
