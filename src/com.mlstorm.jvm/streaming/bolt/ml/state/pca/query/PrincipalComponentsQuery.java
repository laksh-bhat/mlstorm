package bolt.ml.state.pca.query;

import backtype.storm.tuple.Values;
import bolt.ml.state.pca.PrincipalComponentsBase;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.QueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/* license text */


public final class PrincipalComponentsQuery implements QueryFunction<PrincipalComponentsBase, Object> {
    @Override
    public List<Object> batchRetrieve(final PrincipalComponentsBase principalComponentsBase,
                                      final List<TridentTuple> queryTuples) {

        final List<Object> components = new ArrayList<Object>();
        for (TridentTuple ignored : queryTuples) {
            components.add(principalComponentsBase.getPrincipalComponents());
        }
        return components;
    }

    @Override
    public void execute(final TridentTuple queryTuple,
                        final Object component,
                        final TridentCollector tridentCollector) {
        tridentCollector.emit(new Values(component));
    }

    @Override
    public void prepare(final Map map, final TridentOperationContext tridentOperationContext) {
    }

    @Override
    public void cleanup() {
    }
}
