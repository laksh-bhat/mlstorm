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
public class PrincipalComponentsQuery implements QueryFunction<PrincipalComponents, double[][]> {
    @Override
    public List<double[][]> batchRetrieve (final PrincipalComponents principalComponents,
                                           final List<TridentTuple> queryTuples)
    {
        List<double[][]> components = new ArrayList<double[][]>();
        double[][] eigenRowMajor = new double
                [principalComponents.getReverseSensorDictionary().size()]
                [principalComponents.getNumOfPrincipalComponents()];

        for (TridentTuple ignored : queryTuples) {
            int component = 0;
            while (component < principalComponents.getNumOfPrincipalComponents()) {
                double[] basisColumnVector = principalComponents.getBasisVector(component++);
                for (int sensorIndex = 0; sensorIndex < principalComponents.getReverseSensorDictionary().size(); sensorIndex++)
                    eigenRowMajor[sensorIndex][component] = basisColumnVector[sensorIndex];
            }
            components.add(eigenRowMajor);
        }
        return components;
    }

    @Override
    public void execute (final TridentTuple queryTuple,
                         final double[][] component,
                         final TridentCollector tridentCollector)
    {
        tridentCollector.emit(new Values(component));
    }

    @Override
    public void prepare (final Map map, final TridentOperationContext tridentOperationContext) {

    }

    @Override
    public void cleanup () {

    }
}
