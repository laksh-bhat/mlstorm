/* license text */

package topology.weka.linear;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import bolt.ml.state.weka.MlStormWekaState;
import bolt.ml.state.weka.classifier.create.BinaryClassifierFactory;
import bolt.ml.state.weka.classifier.query.BinaryClassifierQuery;
import bolt.ml.state.weka.classifier.update.BinaryClassifierStateUpdater;
import bolt.ml.state.weka.cluster.KmeansClustererState;
import bolt.ml.state.weka.utils.WekaClassificationAlgorithms;
import spout.AustralianElectricityPricingSpout;
import spout.MlStormSpout;
import storm.trident.state.QueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;
import topology.weka.WekaBaseLearningTopology;
import utils.MlStormConfig;
import utils.fields.FieldTemplate;
import utils.fields.MlStormFieldTemplate;

import java.util.logging.Level;
import java.util.logging.Logger;

public class SvmTopology extends WekaBaseLearningTopology {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            Logger.getAnonymousLogger().log(Level.SEVERE, " Where are all the arguments? -- use args -- file numWorkers windowSize parallelism");
            System.exit(-1);
        }

        final FieldTemplate template = new MlStormFieldTemplate();
        final int numWorkers = Integer.valueOf(args[1]);
        final int windowSize = Integer.valueOf(args[2]);
        final int parallelism = Integer.valueOf(args[3]);
        final StateUpdater stateUpdater = new BinaryClassifierStateUpdater(template);
        final StateFactory stateFactory = new BinaryClassifierFactory(WekaClassificationAlgorithms.svm.name(), windowSize, template, null /* weka.core.Utils.splitOptions("-C 1.0 -L 0.0010 -P 1.0E-12 -N 0 -V -1 -W 1 -K \"weka.classifiers.functions.supportVector.PolyKernel -C 250007 -E 1.0\"")*/);
        final QueryFunction<MlStormWekaState, Integer> queryFunction = new BinaryClassifierQuery.SvmQuery();
        final QueryFunction<KmeansClustererState, String> parameterUpdateFunction = null;
        final MlStormSpout features = new AustralianElectricityPricingSpout(args[0], template);
        final StormTopology stormTopology = WekaBaseLearningTopology.buildTopology(features, template, parallelism, stateUpdater, stateFactory, queryFunction, parameterUpdateFunction, "svm", "svmUpdate");

        if (numWorkers == 1) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("SVM", MlStormConfig.getDefaultMlStormConfig(numWorkers), stormTopology);
        } else {
            StormSubmitter.submitTopology("SVM", MlStormConfig.getDefaultMlStormConfig(numWorkers), stormTopology);
        }
    }
}
