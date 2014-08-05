package topology.weka.clustering;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import bolt.ml.state.weka.cluster.CobwebClustererState;
import bolt.ml.state.weka.cluster.create.MlStormClustererFactory;
import bolt.ml.state.weka.cluster.query.MlStormClustererQuery;
import bolt.ml.state.weka.cluster.update.CobwebClusterUpdater;
import spout.MlStormSpout;
import spout.mddb.MddbFeatureExtractorSpout;
import storm.trident.state.QueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;
import topology.weka.WekaBaseLearningTopology;
import utils.MlStormConfig;
import utils.fields.FieldTemplate;
import utils.fields.MlStormFieldTemplate;

import java.util.logging.Level;
import java.util.logging.Logger;


/* license text */

public class CobwebClusteringTopology extends WekaBaseLearningTopology {
    public static final String TOPOLOGY_DRPC_NAME = "CobwebClustering";

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        if (args.length < 3) {
            Logger.getAnonymousLogger().log(Level.SEVERE, "where are the commandline args? -- use args -- folder numWorkers windowSize");
            System.exit(-1);
        }

        final FieldTemplate template = new MlStormFieldTemplate();
        final int numWorkers = Integer.valueOf(args[1]);
        final int windowSize = Integer.valueOf(args[2]);
        final StateUpdater stateUpdater = new CobwebClusterUpdater(template);
        final StateFactory stateFactory = new MlStormClustererFactory.CobwebClustererFactory(numWorkers, windowSize);
        final QueryFunction<CobwebClustererState, String> queryFunction = new MlStormClustererQuery.CobwebClustererQuery();
        final MlStormSpout features = new MddbFeatureExtractorSpout(args[0], template);
        final StormTopology stormTopology = WekaBaseLearningTopology.buildTopology(features, template, numWorkers, stateUpdater, stateFactory, queryFunction, null, TOPOLOGY_DRPC_NAME, null);

        if (numWorkers == 1) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_DRPC_NAME, MlStormConfig.getDefaultMlStormConfig(numWorkers), stormTopology);
        } else {
            StormSubmitter.submitTopology(TOPOLOGY_DRPC_NAME, MlStormConfig.getDefaultMlStormConfig(numWorkers), stormTopology);
        }
    }
}
