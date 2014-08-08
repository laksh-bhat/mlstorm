package topology.weka;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import bolt.general.Printer;
import spout.ml.MlStormSpout;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.QueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;
import utils.fields.FieldTemplate;

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


public abstract class WekaBaseLearningTopology {

    /**
     * @param spout                   An MlStorm spout
     * @param fieldTemplate           A FieldTemplate
     * @param parallelism
     * @param stateUpdater
     * @param stateFactory
     * @param queryFunction
     * @param parameterUpdateFunction
     * @param drpcFunctionName
     * @param drpcUpdateFunction
     * @return
     */
    protected static StormTopology buildTopology(final MlStormSpout spout,
                                                 final FieldTemplate fieldTemplate,
                                                 final int parallelism,
                                                 final StateUpdater stateUpdater,
                                                 final StateFactory stateFactory,
                                                 final QueryFunction queryFunction,
                                                 final QueryFunction parameterUpdateFunction,
                                                 final String drpcFunctionName,
                                                 final String drpcUpdateFunction) {
        final TridentTopology topology = new TridentTopology();
        final Stream featuresStream = topology.newStream("featureVectors", spout);

        /**
         * Stream the feature vectors using the given spout.
         * Use the feature vectors to update a weka classifier/clusterer
         */
        TridentState state =
                featuresStream
                        .broadcast()
                        .parallelismHint(parallelism)
                        .partitionPersist(stateFactory, new Fields(fieldTemplate.getKeyField(), fieldTemplate.getFeatureVectorField()), stateUpdater)
                        .parallelismHint(parallelism);

        // This queries the partition for partitionId and cluster distribution.
        topology.newDRPCStream(drpcFunctionName)
                .broadcast()
                .stateQuery(state, new Fields(FieldTemplate.FieldConstants.ARGS), queryFunction, new Fields(FieldTemplate.FieldConstants.PARTITION, FieldTemplate.FieldConstants.RESULT))
                .toStream()
                .project(new Fields(FieldTemplate.FieldConstants.RESULT))
        ;

        /**
         * The human feedback controller can look at the cluster distributions and later update the parameters (k for kmeans)
         * as a Drpc query. (<partitionId,newK>) ex: args="1, 20". The partitionId is the value returned by the previous query.

         * Notice that this function is generic and one could inject *any* parameter updater functions
         */

        if (parameterUpdateFunction != null) {
            topology.newDRPCStream(drpcUpdateFunction)
                    .broadcast()
                    .stateQuery(state, new Fields(FieldTemplate.FieldConstants.ARGS), parameterUpdateFunction, new Fields(FieldTemplate.FieldConstants.RESULT))
                    .each(new Fields(FieldTemplate.FieldConstants.RESULT), new Printer())
            ;
        }
        return topology.build();
    }
}
