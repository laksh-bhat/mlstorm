package topology.weka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import bolt.ml.state.weka.MlStormWekaState;
import bolt.ml.state.weka.classifier.create.BinaryClassifierFactory;
import bolt.ml.state.weka.classifier.query.BinaryClassifierQuery;
import bolt.ml.state.weka.classifier.update.BinaryClassifierStateUpdater;
import bolt.ml.state.weka.cluster.KmeansClustererState;
import bolt.ml.state.weka.utils.WekaClassificationAlgorithms;
import com.google.common.collect.Lists;
import spout.AustralianElectricityPricingSpout;
import storm.trident.state.QueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;

/**
 * Created by lbhat@DaMSl on 4/21/14.
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
public class SvmTopology extends WekaBaseLearningTopology {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println(" Where are all the arguments? -- use args -- file numWorkers windowSize parallelism");
            return;
        }

        /* The fields our spout is going to emit. These field names are used by the State updaters, so edit with caution */
        String[] fields = {"keyField", "featureVectorField"};
        int numWorkers = Integer.valueOf(args[1]);
        int windowSize = Integer.valueOf(args[2]);
        int parallelism = Integer.valueOf(args[3]);
        StateUpdater stateUpdater = new BinaryClassifierStateUpdater();
        StateFactory stateFactory = new BinaryClassifierFactory(WekaClassificationAlgorithms.svm.name(), windowSize, null /*
                weka.core.Utils.splitOptions("-C 1.0 -L 0.0010 -P 1.0E-12 -N 0 -V -1 -W 1 -K \"weka.classifiers.functions.supportVector.PolyKernel -C 250007 -E 1.0\"")*/);
        QueryFunction<MlStormWekaState, Integer> queryFunction = new BinaryClassifierQuery.SvmQuery();
        QueryFunction<KmeansClustererState, String> parameterUpdateFunction = null;
        IRichSpout features = new AustralianElectricityPricingSpout(args[0], fields);
        StormTopology stormTopology = buildTopology(features, parallelism, stateUpdater, stateFactory, queryFunction, parameterUpdateFunction, "svm", "svmUpdate");

        if (numWorkers == 1) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("SVM", getStormConfig(numWorkers), stormTopology);
        } else StormSubmitter.submitTopology("SVM", getStormConfig(numWorkers), stormTopology);
    }

    public static Config getStormConfig(int numWorkers) {
        Config conf = new Config();
        conf.setNumAckers(numWorkers);
        conf.setNumWorkers(numWorkers);
        conf.setMaxSpoutPending(5); // This is critical; if you don't set this, it's likely that you'll run out of memory and storm will throw wierd errors
        conf.put("topology.spout.max.batch.size", 1000 /* x1000 i.e. every tuple has 1000 feature vectors*/);
        conf.put("topology.trident.batch.emit.interval.millis", 500);
        conf.put(Config.DRPC_SERVERS, Lists.newArrayList("qp-hd3", "qp-hd4", "qp-hd5", "qp-hd6"));
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        conf.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 30);
        return conf;
    }
}
