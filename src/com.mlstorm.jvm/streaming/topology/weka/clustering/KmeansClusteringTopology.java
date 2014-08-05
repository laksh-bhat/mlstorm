package topology.weka.clustering;

/* license text */

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import bolt.ml.state.weka.cluster.KmeansClustererState;
import bolt.ml.state.weka.cluster.create.MlStormClustererFactory;
import bolt.ml.state.weka.cluster.query.MlStormClustererQuery;
import bolt.ml.state.weka.cluster.update.KmeansClusterUpdater;
import spout.MlStormSpout;
import spout.mddb.MddbFeatureExtractorSpout;
import storm.trident.state.QueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;
import topology.weka.WekaBaseLearningTopology;
import utils.MlStormConfig;
import utils.fields.FieldTemplate;
import utils.fields.MlStormFieldTemplate;


public class KmeansClusteringTopology extends WekaBaseLearningTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        if (args.length < 5) {
            System.err.println(" Where are all the arguments? -- use args -- folder numWorkers windowSize k parallelism");
            return;
        }

        final FieldTemplate template = new MlStormFieldTemplate();
        final int numWorkers = Integer.valueOf(args[1]);
        final int windowSize = Integer.valueOf(args[2]);
        final int k = Integer.valueOf(args[3]);
        final int parallelism = Integer.valueOf(args[4]);
        final StateUpdater stateUpdater = new KmeansClusterUpdater(template);
        final StateFactory stateFactory = new MlStormClustererFactory.KmeansClustererFactory(k, windowSize, template);
        final QueryFunction<KmeansClustererState, String> queryFunction = new MlStormClustererQuery.KmeansClustererQuery();
        final QueryFunction<KmeansClustererState, String> parameterUpdateFunction = new MlStormClustererQuery.KmeansNumClustersUpdateQuery();
        final MlStormSpout features = new MddbFeatureExtractorSpout(args[0], template);
        final StormTopology stormTopology = buildTopology(features, template, parallelism, stateUpdater, stateFactory, queryFunction, parameterUpdateFunction, "kmeans", "kUpdate");

        if (numWorkers == 1) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kmeans", MlStormConfig.getDefaultMlStormConfig(numWorkers), stormTopology);
        } else {
            StormSubmitter.submitTopology("kmeans", MlStormConfig.getDefaultMlStormConfig(numWorkers), stormTopology);
        }
    }
}

