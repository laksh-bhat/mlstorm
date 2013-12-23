package bolt.ml.state.weka.cluster.query;

import backtype.storm.tuple.Values;
import bolt.ml.state.weka.cluster.CobwebClustererState;
import bolt.ml.state.weka.cluster.KmeansClustererState;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.QueryFunction;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/16/13
 * Time: 7:44 PM
 */
public class ClustererQuery {

    public static class CobwebClustererQuery implements QueryFunction<CobwebClustererState, String> {
        @Override
        public List<String> batchRetrieve(final CobwebClustererState clustererState, final List<TridentTuple> queryTuples) {
            List<String> queryResults = new ArrayList<String>();
            for (TridentTuple query : queryTuples) {
                if (query.getStringByField("args").split(",")[0].trim().equals(String.valueOf(clustererState.getNumClusters()))) {

                }
            }
            return queryResults;
        }

        @Override
        public void execute(final TridentTuple tuple, final String label, final TridentCollector collector) {
            collector.emit(new Values(label));
        }

        @Override
        public void prepare(final Map map, final TridentOperationContext tridentOperationContext) {
        }

        @Override
        public void cleanup() {
        }
    }


    public static class KmeansClustererQuery implements QueryFunction<KmeansClustererState, String> {
        private int localPartition, numPartitions;

        @Override
        public List<String> batchRetrieve(final KmeansClustererState clustererState, final List<TridentTuple> queryTuples) {
            List<String> queryResults = new ArrayList<String>();
            for (TridentTuple ignored : queryTuples) {
                queryResults.add(Arrays.toString(clustererState.getClusterer().getClusterSizes()));
            }
            return queryResults;
        }

        @Override
        public void execute(final TridentTuple tuple, final String result, final TridentCollector collector) {
            collector.emit(new Values(localPartition, result));
        }

        @Override
        public void prepare(final Map map, final TridentOperationContext tridentOperationContext) {
            localPartition = tridentOperationContext.getPartitionIndex();
            numPartitions = tridentOperationContext.numPartitions();
        }

        @Override
        public void cleanup() {
        }
    }

    public static class KmeansNumClustersUpdateQuery implements QueryFunction<KmeansClustererState, String> {
        private int localPartition, numPartitions;

        @Override
        public List<String> batchRetrieve(final KmeansClustererState clustererState, final List<TridentTuple> queryTuples) {
            List<String> queryResults = new ArrayList<String>();
            System.err.println(MessageFormat.format("DEBUG: KmeansNumClustersUpdateQuery ({0})", localPartition));
            for (TridentTuple args : queryTuples) {
                String query = args.getStringByField("args");
                String[] queryParts = query.split(",");
                int partitionToBeUpdated = Integer.valueOf(queryParts[0].trim());
                int newK = Integer.valueOf(queryParts[1].trim());
                try {
                    if (partitionToBeUpdated == localPartition) {
                        System.err.println("DEBUG: updating local partition " + localPartition);
                        clustererState.updateClustererNumClusters(newK);

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                queryResults.add(MessageFormat.format("update request received at [{0}]", localPartition));
            }
            return queryResults;
        }

        @Override
        public void execute(final TridentTuple tuple, final String result, final TridentCollector collector) {
            collector.emit(new Values(result));
        }

        @Override
        public void prepare(final Map map, final TridentOperationContext tridentOperationContext) {
            localPartition = tridentOperationContext.getPartitionIndex();
            numPartitions = tridentOperationContext.numPartitions();
        }

        @Override
        public void cleanup() {
        }
    }
}
