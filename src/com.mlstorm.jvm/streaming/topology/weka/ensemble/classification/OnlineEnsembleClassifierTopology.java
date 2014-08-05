package topology.weka.ensemble.classification;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import bolt.ml.state.weka.MlStormWekaState;
import bolt.ml.state.weka.classifier.create.BinaryClassifierFactory;
import bolt.ml.state.weka.classifier.query.BinaryClassifierQuery;
import bolt.ml.state.weka.classifier.update.BinaryClassifierStateUpdater;
import bolt.ml.state.weka.classifier.update.MetaFeatureVectorBuilder;
import bolt.ml.state.weka.cluster.query.EnsembleLabelDistributionPairAggregator;
import bolt.ml.state.weka.utils.WekaOnlineClassificationAlgorithms;
import spout.AustralianElectricityPricingSpout;
import spout.MlStormSpout;
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

/* license text */
public class OnlineEnsembleClassifierTopology extends EnsembleLearnerTopologyBuilder {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        if (args.length < 5) {
            System.err.println(" Where are all the arguments? -- use args -- trainingFile numWorkers windowSize k parallelism");
            return;
        }

        final int numWorkers = Integer.valueOf(args[1]);
        final int windowSize = Integer.valueOf(args[2]);
        final int parallelism = Integer.valueOf(args[3]);


        final String drpcFunctionName = "OnlineClassifierEnsemble";
        final FieldTemplate template = new MlStormFieldTemplate();
        final StateUpdater stateUpdater = new BinaryClassifierStateUpdater.BinaryMetaClassifierStateUpdater(template);
        /* All the weak learners and meta learner are Updateable/online learners */
        final StateFactory metaFactory = new BinaryClassifierFactory.OnlineBinaryClassifierFactory(WekaOnlineClassificationAlgorithms.onlineDecisionTree.name(), windowSize, null /* additional options to this weka algorithm */);
        final QueryFunction metaQueryFunction = new BinaryClassifierQuery.MetaQuery();
        final Aggregator metaFeatureVectorBuilder = new MetaFeatureVectorBuilder();
        final ReducerAggregator drpcPartitionResultAggregator = new EnsembleLabelDistributionPairAggregator();
        final QueryFunction<MlStormWekaState, Map.Entry<Integer, double[]>> queryFunction = new BinaryClassifierQuery();

        final List<StateUpdater> stateUpdaters = new ArrayList<StateUpdater>();
        final List<StateFactory> stateFactories = new ArrayList<StateFactory>();
        final List<QueryFunction> queryFunctions = new ArrayList<QueryFunction>();
        final List<String> queryFunctionNames = new ArrayList<String>();

        // Build an ensemble of all the classifiers available in WekaOnlineClassificationAlgorithms
        for (WekaOnlineClassificationAlgorithms alg : WekaOnlineClassificationAlgorithms.values()) {
            stateFactories.add(new BinaryClassifierFactory.OnlineBinaryClassifierFactory(alg.name(), windowSize, null));
            stateUpdaters.add(stateUpdater);
            queryFunctions.add(queryFunction);
            queryFunctionNames.add(drpcFunctionName);
        }

        final MlStormSpout features = new AustralianElectricityPricingSpout(args[0], template);
        /*
        *  This is where we actually build our concrete topology
        *  Take a look at the utils.Base class for detailed description of the arguments and the topology construction details
        */
        final StormTopology stormTopology = EnsembleLearnerTopologyBuilder.buildTopology(features, parallelism, template, stateUpdaters, stateFactories, queryFunctions, queryFunctionNames, drpcPartitionResultAggregator, metaFactory, stateUpdater, metaQueryFunction, metaFeatureVectorBuilder);

        if (numWorkers == 1) {
            final LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(drpcFunctionName, MlStormConfig.getDefaultMlStormConfig(numWorkers), stormTopology);
        } else {
            StormSubmitter.submitTopology(drpcFunctionName, MlStormConfig.getDefaultMlStormConfig(numWorkers), stormTopology);
        }
    }

}
