package bolt.ml.state.ipca.query;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/16/13
 * Time: 3:47 PM
 */
public class PrincipalComponentsAggregator implements CombinerAggregator<double[][]> {
    @Override
    public double[][] init (final TridentTuple components) {
        if (components.getValueByField("components") != null)
            return  (double[][]) components.getValueByField("components");
        else return null;
        
        //double[][] initialPrimitive = new double[initial.length][initial[0].length];
        //for (int i =0 ; i < initial.length; i++){
        //    initialPrimitive[i] = ArrayUtils.toPrimitive(initial[i]);
        //}
    }

    @Override
    public double[][] combine (final double[][] partition1, final double[][] partition2) {
        if (partition1 == null) return partition2;
        else if (partition2 == null) return partition1;

        double[][] combined = new double[partition1.length + partition2.length][partition1[0].length];
        int i = 0;
        for (; i < partition1.length; i++)
            System.arraycopy(partition1, i, combined, i, partition1[i].length);

        for (int j = 0; j < partition2.length && i < combined.length; i++, j++)
            System.arraycopy(partition2, j, combined, i, partition2[j].length);

        return combined;
    }

    @Override
    public double[][] zero () {
        return null;
    }
}
