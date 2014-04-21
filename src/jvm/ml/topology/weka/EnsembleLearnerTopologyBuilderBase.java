package topology.weka;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.Aggregator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.state.QueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lbhat@DaMSl on 2/10/14.
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

public abstract class EnsembleLearnerTopologyBuilderBase {
    /**
     * @param spout The Spout that injects tuples into this topology
     * @param parallelism threads per bolt
     * @param weakAlgorithmStateUpdaters A List of StateUpdaters having 1-1 correspondence to states created by List of
     *                                   StateFactories
     * @param weakAlgorithmStateFactories A List of StateFactories responsible for initializing states
     * @param weakAlgorithmQueryFunctions A list of QueryFunctions that query the State created by above StateFactories
     * @param drpcQueryFunctionNames A list of query-function-names that a drpc client can use to query the states in
     *                               the topology
     * @param drpcPartitionResultAggregator An aggregator to collect results from base/weak learners
     * @param metaStateFactory StateFactory responsible for initializing state for meta algorithm
     * @param metaStateUpdater StateUpdater responsible for updating meta learner state
     * @param metaQueryFunction QueryFunction that queries the meta State created by above meta-StateFactory
     * @param metaFeatureVectorBuilder An aggregator responsible for building a feature vector for the meta learner by
     *                                 combining results from the base learners
     */
    protected static StormTopology buildTopology(final IRichSpout spout,
                                                 final int parallelism,
                                                 final List<StateUpdater> weakAlgorithmStateUpdaters,
                                                 final List<StateFactory> weakAlgorithmStateFactories,
                                                 final List<QueryFunction> weakAlgorithmQueryFunctions,
                                                 final List<String> drpcQueryFunctionNames,
                                                 final ReducerAggregator drpcPartitionResultAggregator,
                                                 final StateFactory metaStateFactory,
                                                 final StateUpdater metaStateUpdater,
                                                 final QueryFunction metaQueryFunction,
                                                 final Aggregator metaFeatureVectorBuilder) {
        assertArguments(spout, parallelism, weakAlgorithmStateUpdaters, weakAlgorithmStateFactories, weakAlgorithmQueryFunctions, drpcQueryFunctionNames, drpcPartitionResultAggregator, metaStateFactory, metaStateUpdater, metaQueryFunction);

        /**
         * Stream the feature vectors using the given spout.
         * Use the feature vectors to update a weka classifiers/clusterers
         */

        TridentTopology topology                = new TridentTopology();
        Stream featuresStream                   = topology.newStream("ensembleStream", spout);

        // create a stream of feature vectors and broadcast them to all partitions (learners/clusterers)
        Stream broadcastStream =
                featuresStream
                        .broadcast()
                        .parallelismHint(parallelism);

        // create individual learner states and persist them
        final List<TridentState> ensembleStates = new ArrayList<TridentState>();
        for (int i = 0; i < weakAlgorithmStateUpdaters.size(); i++) {
            ensembleStates.add(i,
                    broadcastStream.partitionPersist(
                            weakAlgorithmStateFactories.get(i),
                            clustererUpdaterFields,
                            weakAlgorithmStateUpdaters.get(i),
                            partitionProjectionFields
                    ).parallelismHint(parallelism))
            ;
        }

        final List<Stream> streamsToMerge = new ArrayList<Stream>();
        // Accumulate all the streams to be merged to be processed by meta clusterer
        for (TridentState ensembleState : ensembleStates) streamsToMerge.add(ensembleState.newValuesStream());

        // create meta state by reducing outputs from base learners/clusterers
        TridentState metaState = topology.merge(streamsToMerge)
                .groupBy(keyField)
                // NOTE: Aggregator adds the grouping field to the OutputFields
                .aggregate(partitionProjectionFields, metaFeatureVectorBuilder, featureVectorField)
                .global() // Meta classifier/clusterer is not distributed.
                .partitionPersist(metaStateFactory, clustererUpdaterFields, metaStateUpdater)
                .parallelismHint(parallelism);

        // use a single drpc query stream
        final Stream drpcQueryStream = topology.newDRPCStream(drpcQueryFunctionNames.get(0));

        // This queries the partition for partitionId and cluster distribution.
        for (int i = 0; i < weakAlgorithmStateUpdaters.size(); i++) {
            drpcQueryStream
                    .broadcast() // broadcast the query to all partitions
                    .stateQuery(ensembleStates.get(i), drpcQueryArgsField, weakAlgorithmQueryFunctions.get(i), partitionQueryOutputFields)
                    .toStream()
                    .aggregate(partitionQueryOutputFields, drpcPartitionResultAggregator, candidateVotesField)
                    .stateQuery(metaState, candidateVotesField, metaQueryFunction, finalVoteField)
                    .project(finalVoteField)
                    .parallelismHint(parallelism)
            ;
        }

        return topology.build();
    }

    private static void assertArguments(IRichSpout spout, int parallelism, List<StateUpdater> stateUpdaters, List<StateFactory> stateFactories, List<QueryFunction> queryFunctions, List<String> drpcQueryFunctionNames, ReducerAggregator drpcPartitionResultAggregator, StateFactory metaStateFactory, StateUpdater metaStateUpdater, QueryFunction metaQueryFunction) {
        assert spout != null;
        assert parallelism != 0;
        assert stateFactories != null;
        assert queryFunctions != null;
        assert drpcPartitionResultAggregator != null;
        assert stateUpdaters != null;
        assert drpcQueryFunctionNames != null;
        assert metaQueryFunction != null;
        assert metaStateFactory != null;
        assert metaStateUpdater != null;
    }

    public static final Fields keyField = new Fields("keyField");
    public static final Fields finalVoteField = new Fields("finalVoteField");
    public static final Fields drpcQueryArgsField = new Fields("args");
    public static final Fields featureVectorField = new Fields("featureVectorField");
    public static final Fields candidateVotesField = new Fields("voteMap");
    public static final Fields clustererUpdaterFields     = new Fields("keyField", "featureVectorField");
    public static final Fields partitionProjectionFields  = new Fields("partition", "keyField", "label", "actualLabel");
    public static final Fields partitionQueryOutputFields = new Fields("partition", "result");
}
