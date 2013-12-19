package bolt.ml.state.ipca.query;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/16/13
 * Time: 3:47 PM
 */
public class PrincipalComponentsAggregator implements CombinerAggregator<Double[][]> {
    @Override
    public Double[][] init (final TridentTuple eigen) {
        return (Double[][]) eigen.getValueByField("eigen");
    }

    @Override
    public Double[][] combine (final Double[][] partition1, final Double[][] partition2) {
        Double[][] combined = new Double[partition1.length + partition2.length][partition1[0].length];
        int i = 0;
        for (; i < partition1.length; i++)
            System.arraycopy(partition1, i, combined, i, partition1[i].length);

        for (int j = 0; j < partition2.length && i < combined.length; i++, j++)
            System.arraycopy(partition2, j, combined, i, partition2[j].length);

        return combined;
    }

    @Override
    public Double[][] zero () {
        return new Double[0][];
    }
}
