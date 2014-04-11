package topology.weka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import bolt.ml.state.weka.cluster.ClustererState;
import bolt.ml.state.weka.cluster.create.MlStormClustererFactory;
import bolt.ml.state.weka.cluster.query.EnsembleLabelDistributionPairAggregator;
import bolt.ml.state.weka.cluster.query.MlStormClustererQuery;
import bolt.ml.state.weka.cluster.update.ClustererUpdater;
import bolt.ml.state.weka.utils.WekaClusterers;
import com.google.common.collect.Lists;
import spout.mddb.MddbFeatureExtractorSpout;
import storm.trident.operation.ReducerAggregator;
import storm.trident.state.QueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;

import java.util.ArrayList;
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
public class EnsembleClustererTopology extends EnsembleLearnerTopologyBase {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        if (args.length < 5) {
            System.err.println(" Where are all the arguments? -- use args -- bpti_folder numWorkers windowSize k parallelism");
            return;
        }

        final String[] fields = {"key", "featureVector"};
        final String queryFunctionName = "ClustererEnsemble";

        final int numWorkers = Integer.valueOf(args[1]);
        final int windowSize = Integer.valueOf(args[2]);
        final int k = Integer.valueOf(args[3]);
        final int parallelism = Integer.valueOf(args[4]);

        final StateUpdater stateUpdater = new ClustererUpdater();
        final QueryFunction<ClustererState, Map.Entry<Integer, double[]>> queryFunction = new MlStormClustererQuery.ClustererQuery();

        final List<StateUpdater> stateUpdaters = new ArrayList<StateUpdater>();
        final List<StateFactory> factories = new ArrayList<StateFactory>();
        final List<QueryFunction> queryFunctions = new ArrayList<QueryFunction>();
        final List<String> queryFunctionNames = new ArrayList<String>();

        final ReducerAggregator drpcPartitionResultAggregator =  new EnsembleLabelDistributionPairAggregator();
        final StateUpdater metaStateUpdater = new ClustererUpdater();
        final StateFactory metaStateFactory = new MlStormClustererFactory.ClustererFactory(k, windowSize, WekaClusterers.densityBased.name(), false);
        final QueryFunction metaQueryFunction = new MlStormClustererQuery.MetaQuery();

        for (WekaClusterers alg : WekaClusterers.values()) {
            factories.add(new MlStormClustererFactory.ClustererFactory(k, windowSize, alg.name(), true));
            stateUpdaters.add(stateUpdater);
            queryFunctions.add(queryFunction);
            queryFunctionNames.add(queryFunctionName);
        }

        final IRichSpout features = new MddbFeatureExtractorSpout(args[0], fields);
        final StormTopology stormTopology = buildTopology(features, parallelism, stateUpdaters, factories,
                queryFunctions, queryFunctionNames, drpcPartitionResultAggregator, metaStateFactory, metaStateUpdater, metaQueryFunction);

        if (numWorkers == 1) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(queryFunctionName, getStormConfig(numWorkers), stormTopology);
        } else StormSubmitter.submitTopology(queryFunctionName, getStormConfig(numWorkers), stormTopology);
    }

    public static Config getStormConfig(int numWorkers) {
        Config conf = new Config();
        conf.setNumAckers(numWorkers);
        conf.setNumWorkers(numWorkers);
        conf.setMaxSpoutPending(10);
        conf.put("topology.spout.max.batch.size", 1 /* x1000 i.e. every tuple has 1000 feature vectors*/);
        conf.put("topology.trident.batch.emit.interval.millis", 500);
        conf.put(Config.DRPC_SERVERS, Lists.newArrayList("qp-hd3", "qp-hd4", "qp-hd5", "qp-hd6", "qp-hd7", "qp-hd8", "qp-hd9"));
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        conf.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 30);
        return conf;
    }
}
