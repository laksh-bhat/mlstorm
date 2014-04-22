package bolt.ml.state.weka.classifier.query;

import backtype.storm.tuple.Values;
import bolt.ml.state.weka.MlStormWekaState;
import bolt.ml.state.weka.cluster.query.MlStormClustererQuery;
import org.apache.commons.codec.DecoderException;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.QueryFunction;
import storm.trident.tuple.TridentTuple;
import topology.weka.EnsembleLearnerTopologyBuilderBase;
import weka.core.Instance;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by lbhat@DaMSl on 3/24/14.
 * <p/>
 * Copyright {2013} {Lakshmisha Bhat}
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class BinaryClassifierQuery implements QueryFunction<MlStormWekaState, Map.Entry<Integer, double[]>> {
    private int localPartition, numPartitions;

    @Override
    public List<Map.Entry<Integer, double[]>> batchRetrieve(final MlStormWekaState binaryClassifierState, final List<TridentTuple> queryTuples) {
        List<Map.Entry<Integer, double[]>> queryResults = new ArrayList<Map.Entry<Integer, double[]>>();
        for (TridentTuple queryTuple : queryTuples) {
            double[] fv = getFeatureVectorFromArgs(queryTuple);
            final Instance instance = binaryClassifierState.makeWekaInstance(fv);
            try {
                final int classification = (int) binaryClassifierState.predict(instance);
                final double[] distribution = null;
                queryResults.add(new MlStormClustererQuery.Pair<Integer, double[]> (classification, distribution));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return queryResults;
    }

    @Override
    public void execute(final TridentTuple tuple, final Map.Entry<Integer, double[]> result, final TridentCollector collector) {
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

    private double[] getFeatureVectorFromArgs(TridentTuple queryTuple){
        String args = queryTuple.getStringByField(EnsembleLearnerTopologyBuilderBase.drpcQueryArgsField.get(0));
        try {
            return utils.FeatureVectorUtils.deserializeToFeatureVector(args);
        } catch (DecoderException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static class MetaQuery implements QueryFunction<MlStormWekaState, Map.Entry<Double, double[]>> {
        private int localPartition, numPartitions;

        @Override
        public List<Map.Entry<Double, double[]>> batchRetrieve(final MlStormWekaState clustererState, final List<TridentTuple> queryTuples) {
            ArrayList<Map.Entry<Double,double[]>> queryResults = new ArrayList<Map.Entry<Double, double[]>>();
            for (TridentTuple query : queryTuples) {

                final Map<Integer, Map.Entry<Integer, double[]>> voteMap =
                        (Map<Integer, Map.Entry<Integer, double[]>>) query.getValueByField("voteMap");
                final double[] fv = new double[numPartitions];

                for (Integer key : voteMap.keySet()) fv[key] = voteMap.get(key).getKey();

                try {
                    Instance testInstance = clustererState.makeWekaInstance(fv);
                    double[] distribution = null;
                    double result = clustererState.predict(testInstance);
                    queryResults.add(new MlStormClustererQuery.Pair<Double, double[]>(result, distribution));

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    if (e.toString().contains(MlStormWekaState.NOT_READY_TO_PREDICT)){
                        System.err.println(MessageFormat.format("Not Ready yet! Continue training with - {0}", Arrays.toString(fv)));
                        queryResults.add(new MlStormClustererQuery.Pair<Double, double[]>(fv[fv.length-1], null));
                    }
                    else throw new RuntimeException(e);
                }
            }
            return queryResults;
        }

        @Override
        public void execute(TridentTuple tuple, Map.Entry<Double, double[]> result, TridentCollector collector) {
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

    public static class SvmQuery implements QueryFunction<MlStormWekaState,Integer> {
        private int localPartition, numPartitions;

        @Override
        public List<Integer> batchRetrieve(final MlStormWekaState binaryClassifierState, final List<TridentTuple> queryTuples) {
            List<Integer> queryResults = new ArrayList<Integer>();
            for (TridentTuple queryTuple : queryTuples) {
                double[] fv = getFeatureVectorFromArgs(queryTuple);
                final Instance instance = binaryClassifierState.makeWekaInstance(fv);
                try {
                    final int classification = (int) binaryClassifierState.predict(instance);
                    queryResults.add(classification);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return queryResults;
        }

        @Override
        public void execute(final TridentTuple tuple, final Integer result, final TridentCollector collector) {
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

        private double[] getFeatureVectorFromArgs(TridentTuple queryTuple){
            String args = queryTuple.getStringByField(EnsembleLearnerTopologyBuilderBase.drpcQueryArgsField.get(0));
            try {
                return utils.FeatureVectorUtils.deserializeToFeatureVector(args);
            } catch (DecoderException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

}
