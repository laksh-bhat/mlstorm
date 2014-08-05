package bolt.ml.state.pca.query;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;
import utils.fields.FieldTemplate;

/* license text */


public class PrincipalComponentsAggregator implements CombinerAggregator<Double[][]> {
    @Override
    public Double[][] init(final TridentTuple components) {
        if (components.getValueByField(FieldTemplate.FieldConstants.PCA.PCA_COMPONENTS) != null) {
            return (Double[][]) components.getValueByField(FieldTemplate.FieldConstants.PCA.PCA_COMPONENTS);
        } else {
            return null;
        }
    }

    @Override
    public Double[][] combine(final Double[][] partition1, final Double[][] partition2) {
        if (partition1 == null) {
            return partition2;
        } else if (partition2 == null) {
            return partition1;
        }

        Double[][] combined = new Double[partition1.length + partition2.length][partition1[0].length];
        int i = 0;
        for (; i < partition1.length; i++) {
            System.arraycopy(partition1, i, combined, i, partition1[i].length);
        }

        for (int j = 0; j < partition2.length && i < combined.length; i++, j++) {
            System.arraycopy(partition2, j, combined, i, partition2[j].length);
        }

        return combined;
    }

    @Override
    public Double[][] zero() {
        return null;
    }
}
