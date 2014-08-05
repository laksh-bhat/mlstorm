package bolt.ml.state.weka.cluster.query;

import backtype.storm.tuple.Values;
import optimization.HungarianAlgorithm;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/* license text */
public class HungarianMethodBasedVoting implements Function {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        @SuppressWarnings("unchecked")
        HashMap<Integer, Map.Entry<Integer, double[]>> voteMap = (HashMap<Integer, Map.Entry<Integer, double[]>>) tuple.getValueByField("voteMap");

        int k = 0;
        final int partitions = voteMap.keySet().size();
        for (Integer key : voteMap.keySet()) {
            final double[] val = voteMap.get(key).getValue();
            k = val.length;
        }

        final double[][] costMatrix = new double[k][k];
        for (int i /* partitions */ = 0; i < partitions; i++) {
            for (int j /* clusters */ = 0; j < k; j++) {
                costMatrix[i][j] = 1.0 - voteMap.get(i).getValue()[j];
            }
        }
        final int[] partitionLabelling = new HungarianAlgorithm(costMatrix).execute();
        collector.emit(new Values(partitionLabelling));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        Logger.getAnonymousLogger().log(Level.INFO, MessageFormat.format("Aggregating based on hungarian method at {0} of {1}", context.getPartitionIndex(), context.numPartitions()));
    }

    @Override
    public void cleanup() {
    }
}
