package bolt.ml.state.ipca.query;

import bolt.ml.state.ipca.PrincipalComponents;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.QueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/16/13
 * Time: 12:28 PM
 */
public class PrincipalComponentsQuery implements QueryFunction<PrincipalComponents, Double[][]>{
    @Override
    public List<Double[][]> batchRetrieve (final PrincipalComponents principalComponents,
                                           final List<TridentTuple> tridentTuples)
    {
        return null;
    }

    @Override
    public void execute (final TridentTuple objects,
                         final Double[][] doubles,
                         final TridentCollector tridentCollector)
    {

    }

    @Override
    public void prepare (final Map map, final TridentOperationContext tridentOperationContext) {

    }

    @Override
    public void cleanup () {

    }
}
