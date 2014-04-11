package bolt.ml.state.ipca.update;

import bolt.ml.state.ipca.PrincipalComponentsBase;
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

public class PrincipalComponentUpdater implements StateUpdater<PrincipalComponentsBase> {
    int localPartition, numPartitions;

    @Override
    public void updateState (final PrincipalComponentsBase state,
                             final List<TridentTuple> tuples,
                             final TridentCollector collector)
    {
        for (TridentTuple tuple : tuples) {
            String sensor = tuple.getStringByField("sensor");
            Double data = (Double) tuple.getValueByField("sensorData");
            //System.err.println(MessageFormat.format("DEBUG: Updating state: sensor name is {0} and temperature is {1}", sensor, data));
            Map<String, Double> sensors = state.getCurrentSensors();

            if (sensors.containsKey(sensor)) {
                Double existingValue = sensors.get(sensor);
                sensors.put(sensor, (existingValue + data) / 2.0);
            } else sensors.put(sensor, data);
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
