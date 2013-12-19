package bolt.ml.state.ipca.update;

import bolt.ml.state.ipca.PrincipalComponents;
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
    int localPartition, numPartitions;

    @Override
    public void updateState (final PrincipalComponents state,
                             final List<TridentTuple> tuples,
                             final TridentCollector collector)
    {
        for (TridentTuple tuple : tuples) {
            String key = tuple.getStringByField("sensor");
            Double value = (Double) tuple.getValueByField("sensorData");
            Map<String, Double> sensors = state.getFeatureVectors();

            if (sensors.containsKey(key)) {
                Double existingValue = sensors.get(key);
                sensors.put(key, (existingValue + value) / 2.0);
            } else sensors.put(key, value);
        }
    }

    @Override
    public void prepare (final Map conf, final TridentOperationContext context) {
        localPartition = context.getPartitionIndex();
        numPartitions = context.numPartitions();
    }

    @Override
    public void cleanup () {}
}
