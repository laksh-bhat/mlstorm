package topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import bolt.ml.state.pca.create.PcaFactory;
import bolt.ml.state.pca.query.PrincipalComponentsAggregator;
import bolt.ml.state.pca.query.PrincipalComponentsQuery;
import bolt.ml.state.pca.update.AggregateFilter;
import bolt.ml.state.pca.update.PrincipalComponentUpdater;
import bolt.ml.state.pca.update.PrincipalComponentsRefresher;
import spout.text.FileStreamingSpout;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

/**
 * User: lbhat@DaMSl on 12/12/13.
 * Time: 9:53 PM
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
 * distributed under the License is distributed an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

public class PcaTopology {
    public static void main (String[] args) {
        String filename = args[0];
        String[] fields = {"key", "sensorData"};
        int parallelism = args.length > 1 ? Integer.valueOf(args[1]) : 8;

        StormTopology stormTopology = buildTopology(parallelism, filename, fields, 1000 /*noOfSamplesInWindow*/, 2 /*noOfFeatures*/);
    }

    private static StormTopology buildTopology (final int parallelism,
                                                final String filename,
                                                final String[] fields,
                                                final int numSamples,
                                                final int elementsInSample)
    {
        IRichSpout sensorSpout = new FileStreamingSpout(filename, fields);
        TridentTopology topology = new TridentTopology();
        Stream sensorStream = topology.newStream("sensor", sensorSpout);
        StateFactory pcaFactory = new PcaFactory(numSamples, elementsInSample);

        TridentState principalComponents = sensorStream.partitionBy(new Fields("sensor"))
                                                       .partitionPersist(pcaFactory, new Fields("key", "sensorData"), new PrincipalComponentUpdater())
                                                       .parallelismHint(parallelism);


        Stream principalComponentsStream = topology.newDRPCStream("PCA")
                                                   .broadcast()
                                                   .stateQuery(principalComponents, new Fields("args"), new PrincipalComponentsQuery(), new Fields("eigen"));


        principalComponentsStream
                .aggregate(new Fields("eigen"), new PrincipalComponentsAggregator(), new Fields("refreshedEigen"))
                .each(new Fields("refreshedEigen"), new AggregateFilter(), new Fields("eigen"))
                .project(new Fields("eigen"))
                .broadcast()
                .partitionPersist(pcaFactory, new Fields("eigen"), new PrincipalComponentsRefresher())
                .parallelismHint(parallelism)
        ;

        return topology.build();
    }
}
