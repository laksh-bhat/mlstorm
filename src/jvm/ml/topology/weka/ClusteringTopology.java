package topology.weka;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import bolt.ml.state.weka.cluster.ClustererState;
import bolt.ml.state.weka.cluster.create.ClustererFactory;
import bolt.ml.state.weka.cluster.query.ClustererQuery;
import bolt.ml.state.weka.cluster.update.ClusterUpdater;
import com.google.common.collect.Lists;
import spout.mddb.MddbFeatureExtractorSpout;
import storm.trident.state.QueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;

import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;


/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/17/13
 * Time: 5:05 PM
 */

public class ClusteringTopology extends WekaLearningBaseTopology {
    public static void main (String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Logger logger = LogManager.getLogManager().getLogger("");
        if (args.length < 3) {
            logger.log(Level.ALL, "-- use args -- folder numWorkers");
            return;
        }

        String[] fields = {"key", "featureVector"};
        int numWorkers = Integer.valueOf(args[1]);
        StateUpdater stateUpdater = new ClusterUpdater();
        StateFactory stateFactory = new ClustererFactory(numWorkers);
        QueryFunction<ClustererState, String> queryFunction = new ClustererQuery();
        IRichSpout features = new MddbFeatureExtractorSpout(args[0], fields);
        StormTopology stormTopology = buildTopology(features, numWorkers, stateUpdater, stateFactory, queryFunction, "clusterer");
        StormSubmitter.submitTopology("learner", getStormConfig(numWorkers), stormTopology);
    }

    public static Config getStormConfig (int numWorkers) {
        Config conf = new Config();
        conf.setNumAckers(numWorkers);
        conf.setNumWorkers(numWorkers);
        conf.setMaxSpoutPending(1);
        conf.put("topology.spout.max.batch.size", 1 /* x1000 i.e. every tuple has 1000 feature vectors*/ );
        conf.put("topology.trident.batch.emit.interval.millis", 5000);
        conf.put(Config.DRPC_SERVERS, Lists.newArrayList("qp-hd3", "qp-hd4", "qp-hd5", "qp-hd6", "qp-hd7", "qp-hd8", "qp-hd9"));
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        conf.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 120);
        return conf;
    }
}
