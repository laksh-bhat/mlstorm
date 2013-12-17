package bolt.ml.state.pca.update;

import bolt.ml.state.pca.PrincipalComponents;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/16/13
 * Time: 12:36 PM
 */
public class PrincipalComponentsRefresher implements StateUpdater<PrincipalComponents> {
    @Override
    public void updateState (final PrincipalComponents principalComponents,
                             final List<TridentTuple> tuples,
                             final TridentCollector collector)
    {
        for (TridentTuple tuple : tuples){
            // TODO figure out a way to perform state "merge"
            // TODO look at Candid covariance-free incremental principal component analysis (CCIPCA) paper.
            collector.emit(tuple);
        }
    }

    @Override
    public void prepare (final Map map, final TridentOperationContext tridentOperationContext) {

    }

    @Override
    public void cleanup () {

    }
}
