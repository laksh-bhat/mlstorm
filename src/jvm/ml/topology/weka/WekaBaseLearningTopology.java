package topology.weka;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import bolt.general.Printer;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.QueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;

/**
 * Created by lbhat@DaMSl on 12/22/13.
 * <p/>
 * Copyright {2013} {Lakshmisha Bhat <laksh85@gmail.com>}
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


public class WekaBaseLearningTopology {
    protected static StormTopology buildTopology(final IRichSpout spout,
                                                 final int parallelism,
                                                 final StateUpdater stateUpdater,
                                                 final StateFactory stateFactory,
                                                 final QueryFunction queryFunction,
                                                 QueryFunction parameterUpdateFunction,
                                                 final String drpcFunction,
                                                 final String drpcUpdateFunction) {
        TridentTopology topology = new TridentTopology();
        Stream featuresStream = topology.newStream("featureVectorsInWindow", spout);

        /**
         * Stream the feature vectors using the given spout.
         * Use the feature vectors to update a weka classifier/clusterer
         */
        TridentState state =
                featuresStream
                        .broadcast()
                        .parallelismHint(parallelism)
                        .partitionPersist(stateFactory, new Fields("key", "featureVector"), stateUpdater)
                        .parallelismHint(parallelism);

        // This queries the partition for partitionId and cluster distribution.
        topology.newDRPCStream(drpcFunction)
                .broadcast()
                .stateQuery(state, new Fields("args"), queryFunction, new Fields("partition", "result"))
                .toStream()
                .each(new Fields("partition", "result"), new Printer())
        ;

        /**
         * The human feedback controller can look at the cluster distributions and later update the parameters (k for kmeans)
         * as a Drpc query. (<partitionId,newK>) ex: args="1, 20". The partitionId is the value returned by the previous query.

         * Notice that this function is generic and one could inject *any* parameter updater functions
         */

        if (parameterUpdateFunction != null){
            topology.newDRPCStream(drpcUpdateFunction)
                    .broadcast()
                    .stateQuery(state, new Fields("args"), parameterUpdateFunction, new Fields("result"))
                    .each(new Fields("result"), new Printer())
            ;
        }


        return topology.build();
    }

}
