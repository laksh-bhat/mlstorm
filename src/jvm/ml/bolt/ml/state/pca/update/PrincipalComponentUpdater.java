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
 * Date: 12/13/13
 * Time: 10:55 PM
 */
public class PrincipalComponentUpdater implements StateUpdater<PrincipalComponents> {

    @Override
    public void updateState (final PrincipalComponents state,
                             final List<TridentTuple> tuples,
                             final TridentCollector collector)
    {
        for (TridentTuple tuple : tuples){
            state.getFeatures().put(tuple.getIntegerByField("integer"), (Double[]) tuple.getValueByField("sensorData"));
        }
    }

    @Override
    public void prepare (final Map conf, final TridentOperationContext context) {

    }

    @Override
    public void cleanup () {

    }
}
