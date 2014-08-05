package topology.weka.ensemble.consensus.clustering;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import bolt.ml.state.weka.cluster.ClustererState;
import bolt.ml.state.weka.cluster.create.MlStormClustererFactory;
import bolt.ml.state.weka.cluster.query.EnsembleLabelDistributionPairAggregator;
import bolt.ml.state.weka.cluster.query.MlStormClustererQuery;
import bolt.ml.state.weka.cluster.update.ClustererUpdater;
import bolt.ml.state.weka.cluster.update.MetaFeatureVectorBuilder;
import bolt.ml.state.weka.utils.WekaClusterers;
import spout.MlStormSpout;
import spout.mddb.MddbFeatureExtractorSpout;
import storm.trident.operation.Aggregator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.state.QueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;
import topology.weka.ensemble.EnsembleLearnerTopologyBuilder;
import utils.MlStormConfig;
import utils.fields.FieldTemplate;
import utils.fields.MlStormFieldTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/* license text */

/**
 *
 */
public class EnsembleClustererTopology extends EnsembleLearnerTopologyBuilder {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        if (args.length < 5) {
            Logger.getAnonymousLogger().log(Level.SEVERE, " Where are all the arguments? -- use args -- bpti_folder numWorkers windowSize k parallelism");
            System.exit(-1);
        }

        final FieldTemplate template = new MlStormFieldTemplate();

        final int numWorkers = Integer.valueOf(args[1]);
        final int windowSize = Integer.valueOf(args[2]);
        final int k = Integer.valueOf(args[3]);
        final int parallelism = Integer.valueOf(args[4]);

        final StateUpdater stateUpdater = new ClustererUpdater(template);
        final QueryFunction<ClustererState, Map.Entry<Integer, double[]>> queryFunction = new MlStormClustererQuery.ClustererQuery();

        final List<StateUpdater> stateUpdaters = new ArrayList<StateUpdater>();
        final List<StateFactory> factories = new ArrayList<StateFactory>();
        final List<QueryFunction> queryFunctions = new ArrayList<QueryFunction>();
        final List<String> queryFunctionNames = new ArrayList<String>();

        final ReducerAggregator drpcPartitionResultAggregator = new EnsembleLabelDistributionPairAggregator();
        final StateUpdater metaStateUpdater = new ClustererUpdater(template);
        final StateFactory metaStateFactory = new MlStormClustererFactory.ClustererFactory(k, windowSize, WekaClusterers.densityBased.name(), false, template, null /* additional options to this weka algorithm */);
        final QueryFunction metaQueryFunction = new MlStormClustererQuery.MetaClustererQuery();
        final Aggregator metaFeatureVectorBuilder = new MetaFeatureVectorBuilder();

        for (WekaClusterers alg : WekaClusterers.values()) {
            factories.add(new MlStormClustererFactory.ClustererFactory(k, windowSize, alg.name(), true, template, null));
            stateUpdaters.add(stateUpdater);
            queryFunctions.add(queryFunction);
            queryFunctionNames.add(FieldTemplate.FieldConstants.CONSENSUS.CONSENSUS_DRPC);
        }

        final MlStormSpout spout = new MddbFeatureExtractorSpout(args[0], template);
        /*
        *  This is where we actually build our concrete topology
        *  Take a look at the utils.Base class for detailed description of the arguments and the topology construction details
        */
        final StormTopology stormTopology = buildTopology(spout, parallelism, template, stateUpdaters, factories,
                queryFunctions, queryFunctionNames, drpcPartitionResultAggregator, metaStateFactory, metaStateUpdater, metaQueryFunction, metaFeatureVectorBuilder);

        if (numWorkers == 1) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(FieldTemplate.FieldConstants.CONSENSUS.CONSENSUS_DRPC, MlStormConfig.getMddbStormConfig(numWorkers), stormTopology);
        } else {
            StormSubmitter.submitTopology(FieldTemplate.FieldConstants.CONSENSUS.CONSENSUS_DRPC, MlStormConfig.getMddbStormConfig(numWorkers), stormTopology);
        }
    }

}
