package topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import bolt.ml.state.ipca.create.PcaFactory;
import bolt.ml.state.ipca.query.PrincipalComponentsAggregator;
import bolt.ml.state.ipca.query.PrincipalComponentsQuery;
import bolt.ml.state.ipca.update.AggregateFilter;
import bolt.ml.state.ipca.update.PrincipalComponentUpdater;
import bolt.ml.state.ipca.update.PrincipalComponentsRefresher;
import com.google.common.collect.Lists;
import spout.sensor.SensorStreamingSpout;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchExecutor;
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
    public static void main (String[] args) throws AlreadyAliveException, InvalidTopologyException {
        String[] fields = {"sensor", "sensorData"};
        int parallelism = args.length > 1 ? Integer.valueOf(args[0]) : 2;

        StormTopology stormTopology = buildTopology(parallelism, fields, 100);
        StormSubmitter.submitTopology("Pca", getStormConfig(parallelism), stormTopology);
    }

    public static Config getStormConfig (int numWorkers) {
        Config conf = new Config();
        conf.setNumAckers(numWorkers);
        conf.setNumWorkers(numWorkers);
        conf.setMaxSpoutPending(100);
        conf.put("topology.spout.max.batch.size", 1000 );
        conf.put("topology.trident.batch.emit.interval.millis", 5000);
        conf.put(Config.DRPC_SERVERS, Lists.newArrayList("qp-hd3", "qp-hd4", "qp-hd5", "qp-hd6", "qp-hd7", "qp-hd8", "qp-hd9"));
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        conf.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 120);
        return conf;
    }

    private static StormTopology buildTopology (final int parallelism,
                                                final String[] fields,
                                                final int pcaRowWidth)
    {
        IRichSpout sensorSpout = new SensorStreamingSpout(fields);
        ITridentSpout batchSpout = new RichSpoutBatchExecutor(sensorSpout);
        TridentTopology topology = new TridentTopology();
        Stream sensorStream = topology.newStream("sensorSpout", batchSpout);
        StateFactory pcaFactory = new PcaFactory(pcaRowWidth);

        TridentState principalComponents =
                sensorStream
                        .shuffle()
                        .partitionPersist(pcaFactory, new Fields("sensor", "sensorData"), new PrincipalComponentUpdater())
                        .parallelismHint(parallelism);

        Stream principalComponentsStream =
                topology.newDRPCStream("PCA")
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
