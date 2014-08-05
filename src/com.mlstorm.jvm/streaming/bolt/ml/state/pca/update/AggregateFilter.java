package bolt.ml.state.pca.update;

import org.ejml.data.DenseMatrix64F;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import utils.fields.FieldTemplate;

import java.text.MessageFormat;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/16/13
 * Time: 5:17 PM
 */
public class AggregateFilter implements Function {
    @Override
    public void execute(final TridentTuple aggregatedEigen, final TridentCollector collector) {
        double[][] eigen = (double[][]) aggregatedEigen.getValueByField(FieldTemplate.FieldConstants.PCA.PCA_EIGEN);
        DenseMatrix64F eigenMatrix = new DenseMatrix64F(eigen);
        // TODO I'm not sure if I need this Function
    }

    @Override
    public void prepare(final Map map, final TridentOperationContext context) {
        Logger.getAnonymousLogger().log(Level.INFO, MessageFormat.format("AggregatorFilter at {0} of {1}", context.getPartitionIndex(), context.numPartitions()));
    }

    @Override
    public void cleanup() {
    }
}
