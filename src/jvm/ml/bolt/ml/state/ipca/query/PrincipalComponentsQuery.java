package bolt.ml.state.ipca.query;

import backtype.storm.tuple.Values;
import bolt.ml.state.ipca.PrincipalComponents;
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
 * Time: 12:28 PM
 */
public final class PrincipalComponentsQuery implements QueryFunction<PrincipalComponents, Double[][]> {
    @Override
    public List<Double[][]> batchRetrieve(final PrincipalComponents principalComponents,
                                          final List<TridentTuple> queryTuples) {

        final List<Double[][]> components = new ArrayList<Double[][]>();
        for (TridentTuple ignored : queryTuples) components.add(principalComponents.getPrincipalComponents());
        return components;
    }

    @Override
    public void execute(final TridentTuple queryTuple,
                        final Double[][] component,
                        final TridentCollector tridentCollector) {
        tridentCollector.emit(new Values(component));
    }

    @Override
    public void prepare(final Map map, final TridentOperationContext tridentOperationContext) {}

    @Override
    public void cleanup() {}
}
