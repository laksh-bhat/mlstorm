package bolt.ml.state.pca.update;

import bolt.ml.state.pca.PrincipalComponentsBase;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import utils.FeatureVectorUtils;
import utils.Pair;
import utils.fields.FieldTemplate;

import java.util.List;
import java.util.Map;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/13/13
 * Time: 10:55 PM
 */

public class PrincipalComponentUpdater implements StateUpdater<PrincipalComponentsBase> {
    private final FieldTemplate template    ;
    int localPartition, numPartitions;

    public PrincipalComponentUpdater(FieldTemplate template){
        this.template = template;
    }
    @Override
    public void updateState(final PrincipalComponentsBase state,
                            final List<TridentTuple> tuples,
                            final TridentCollector collector) {
        for (TridentTuple tuple : tuples) {
            final Pair<Object, double[]> pair = FeatureVectorUtils.getKeyValuePairFromMlStormFeatureVector(template, tuple);
            final String sensor = (String) pair.getKey();
            final double temperature = pair.getValue()[0];
            final Map<String, Double> sensors = state.getCurrentSensors();
            if (sensors.containsKey(sensor)) {
                double existingValue = sensors.get(sensor);
                sensors.put(sensor, (existingValue + temperature) / 2.0);
            } else {
                sensors.put(sensor, temperature);
            }
        }
    }

    @Override
    public void prepare(final Map conf, final TridentOperationContext context) {
        localPartition = context.getPartitionIndex() + 1;
        numPartitions = context.numPartitions();
    }

    @Override
    public void cleanup() {
    }
}
