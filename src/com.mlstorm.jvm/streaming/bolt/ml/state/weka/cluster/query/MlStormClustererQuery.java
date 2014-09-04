package bolt.ml.state.weka.cluster.query;

import backtype.storm.tuple.Values;
import bolt.ml.state.weka.cluster.ClustererState;
import bolt.ml.state.weka.cluster.CobwebClustererState;
import bolt.ml.state.weka.cluster.KmeansClustererState;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.QueryFunction;
import storm.trident.tuple.TridentTuple;
import utils.MlStormFeatureVectorUtils;
import utils.KeyValuePair;
import weka.core.Instance;
import weka.core.Instances;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

 /*
 * Copyright 2013-2015 Lakshmisha Bhat
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class MlStormClustererQuery {

    public static class MetaClustererQuery implements QueryFunction<ClustererState, Map.Entry<Integer, double[]>> {
        private int localPartition, numPartitions;

        @Override
        public List<Map.Entry<Integer, double[]>> batchRetrieve(final ClustererState clustererState, final List<TridentTuple> queryTuples) {
            ArrayList<Map.Entry<Integer, double[]>> queryResults = new ArrayList<Map.Entry<Integer, double[]>>();
            for (TridentTuple query : queryTuples) {
                final double[] fv = new double[numPartitions];
                final HashMap<Integer, Map.Entry<Integer, double[]>> voteMap = (HashMap<Integer, Map.Entry<Integer, double[]>>) query.getValueByField("voteMap");

                for (Integer key : voteMap.keySet()) {
                    fv[key] = voteMap.get(key).getKey();
                }

                try {
                    final Instance testInstance = MlStormFeatureVectorUtils.buildWekaInstance(fv);
                    final double[] distribution = clustererState.getClusterer().distributionForInstance(testInstance);
                    final Integer result = clustererState.getClusterer().clusterInstance(testInstance);
                    queryResults.add(new KeyValuePair<Integer, double[]>(result, distribution));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return queryResults;
        }

        @Override
        public void execute(TridentTuple tuple, Map.Entry<Integer, double[]> result, TridentCollector collector) {
            collector.emit(new Values(result.getKey()));
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

    public static class ClustererQuery implements QueryFunction<ClustererState, Map.Entry<Integer, double[]>> {
        private int localPartition, numPartitions;

        @Override
        public List<Map.Entry<Integer, double[]>> batchRetrieve(final ClustererState clustererState, final List<TridentTuple> queryTuples) {
            ArrayList<Map.Entry<Integer, double[]>> queryResults = new ArrayList<Map.Entry<Integer, double[]>>();
            for (TridentTuple query : queryTuples) {
                String q = query.getStringByField("args");
                try {
                    final double[] featureVector = MlStormFeatureVectorUtils.deserializeToFeatureVector(q);
                    final Instance testInstance = MlStormFeatureVectorUtils.buildWekaInstance(featureVector);
                    final double[] distribution = clustererState.getClusterer().distributionForInstance(testInstance);
                    final Integer label = clustererState.getClusterer().clusterInstance(testInstance);
                    queryResults.add(new KeyValuePair<Integer, double[]>(label, distribution));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return queryResults;
        }

        @Override
        public void execute(TridentTuple tuple, Map.Entry<Integer, double[]> result, TridentCollector collector) {
            collector.emit(new Values(localPartition, result));
        }

        @Override
        public void prepare(final Map map, final TridentOperationContext tridentOperationContext) {
            localPartition = tridentOperationContext.getPartitionIndex() + 1;
            numPartitions = tridentOperationContext.numPartitions();
        }

        @Override
        public void cleanup() {
        }
    }

    public static class CobwebClustererQuery implements QueryFunction<CobwebClustererState, String> {
        @Override
        public List<String> batchRetrieve(final CobwebClustererState clustererState, final List<TridentTuple> queryTuples) {
            List<String> queryResults = new ArrayList<String>();
            for (TridentTuple query : queryTuples) {
                if (query.getStringByField("args").split(",")[0].trim().equals(String.valueOf(clustererState.getNumClusters()))) {
                    // todo fix this
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
        public static final String CENTROID_DELIM = ",";
        private int localPartition, numPartitions;

        @Override
        public List<String> batchRetrieve(final KmeansClustererState clustererState, final List<TridentTuple> queryTuples) {
            List<String> queryResults = new ArrayList<String>();
            try {
                for (TridentTuple ignored : queryTuples) {
                    Instances centroids = clustererState.getClusterer().getClusterCentroids();
                    StringBuilder serializedClusterCentres = new StringBuilder();
                    for (int i = 0; i < centroids.size(); i++) {
                        Instance centroid = centroids.get(i);
                        String serialized = MlStormFeatureVectorUtils.serializeFeatureVector(centroid.toDoubleArray());
                        if (i != centroids.size() - 1) {
                            serialized += CENTROID_DELIM;
                        }
                        serializedClusterCentres.append(serialized);
                    }

                    queryResults.add(serializedClusterCentres.toString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return queryResults;
        }

        @Override
        public void execute(final TridentTuple tuple, final String result, final TridentCollector collector) {
            collector.emit(new Values(localPartition, result));
        }

        @Override
        public void prepare(final Map map, final TridentOperationContext tridentOperationContext) {
            localPartition = tridentOperationContext.getPartitionIndex() + 1; // partition starts at 0
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
            Logger.getAnonymousLogger().log(Level.INFO, (MessageFormat.format("KmeansNumClustersUpdateQuery ({0})", localPartition)));
            for (TridentTuple args : queryTuples) {
                String query = args.getStringByField("args");
                String[] queryParts = query.split(",");
                int partitionToBeUpdated = Integer.valueOf(queryParts[0].trim());
                int newK = Integer.valueOf(queryParts[1].trim());

                queryResults.add(MessageFormat.format("k update request ({1}->{3}) received at [{0}]; " +
                                "average trainingtime for k = [{1}] = [{2}]ms",
                        localPartition, clustererState.getNumClusters(), clustererState.getStatistics(), newK));
                try {
                    if (partitionToBeUpdated == localPartition) {
                        System.err.println("DEBUG: updating local partition " + localPartition);
                        clustererState.updateClustererNumClusters(newK);

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return queryResults;
        }

        @Override
        public void execute(final TridentTuple tuple, final String result, final TridentCollector collector) {
            collector.emit(new Values(result));
        }

        @Override
        public void prepare(final Map map, final TridentOperationContext tridentOperationContext) {
            localPartition = tridentOperationContext.getPartitionIndex() + 1;
            numPartitions = tridentOperationContext.numPartitions();
        }

        @Override
        public void cleanup() {
        }
    }
}
