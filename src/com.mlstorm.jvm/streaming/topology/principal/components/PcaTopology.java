package topology.principal.components;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import bolt.ml.state.pca.create.WindowedPcaFactory;
import bolt.ml.state.pca.query.PrincipalComponentsAggregator;
import bolt.ml.state.pca.query.PrincipalComponentsQuery;
import bolt.ml.state.pca.update.PrincipalComponentUpdater;
import spout.ml.MlStormSpout;
import spout.ml.sensor.SensorStreamingSpout;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchExecutor;
import storm.trident.state.StateFactory;
import utils.MlStormConfig;
import utils.fields.FieldTemplate;
import utils.fields.MlStormFieldTemplate;

 /*
 * Copyright 2013-2015 Lakshmisha Bhat
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Run PCA algorithm on storm. This provides horizontal scaling.
 * Another IncrementalPCATopology is coming soon that's completely distributed.
 */
public class PcaTopology {
    // The minimum no. of samples required to run the PCA algorithm.
    private static final int PCA_SAMPLE_SIZE = 100;
    // The number of principal components output by the algorithm.
    private static final int PRINCIPAL_COMPONENTS = 5;

    private static StormTopology buildTopology(final MlStormSpout mlStormSpout,
                                               final int parallelism,
                                               final int pcaRowWidth,
                                               final int numPrincipalComponents,
                                               final FieldTemplate template) {
        final TridentTopology topology = new TridentTopology();
        final Stream sensorStream = topology.newStream(FieldTemplate.FieldConstants.PCA.PCA, mlStormSpout);
        final StateFactory pcaFactory = new WindowedPcaFactory(pcaRowWidth, numPrincipalComponents, template);

        final TridentState principalComponents =
                sensorStream
                        .partitionPersist(pcaFactory, new Fields(template.getKeyField(), template.getFeatureVectorField()), new PrincipalComponentUpdater(template))
                        .parallelismHint(parallelism);


        topology.newDRPCStream(FieldTemplate.FieldConstants.PCA.PCA_DRPC)
                .broadcast()
                .stateQuery(principalComponents, new Fields(FieldTemplate.FieldConstants.ARGS), new PrincipalComponentsQuery(), new Fields(FieldTemplate.FieldConstants.PCA.PCA_COMPONENTS))
                .project(new Fields(FieldTemplate.FieldConstants.PCA.PCA_COMPONENTS))
                .aggregate(new Fields(FieldTemplate.FieldConstants.PCA.PCA_COMPONENTS), new PrincipalComponentsAggregator(), new Fields(FieldTemplate.FieldConstants.PCA.PCA_EIGEN))
                .project(new Fields(FieldTemplate.FieldConstants.PCA.PCA_EIGEN));

        return topology.build();
    }

    // This is all the code you need to write to run PCA algorithm. Create your own Spout that reads and emits MlStormFeatureVector.
    // Look at spout.ml.sensor.SensorStreamingSpout or spout.ml.weka.AustralianElectricityPricingSpout for example.
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        final FieldTemplate fieldTemplate = new MlStormFieldTemplate();
        final MlStormSpout spout = new SensorStreamingSpout(fieldTemplate);
        final int parallelism = args.length > 0 ? Integer.valueOf(args[0]) : 1;
        final StormTopology stormTopology = buildTopology(spout, parallelism, PCA_SAMPLE_SIZE, PRINCIPAL_COMPONENTS, fieldTemplate);

        if (parallelism == 1) {
            final LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(FieldTemplate.FieldConstants.PCA.PCA, MlStormConfig.getDefaultMlStormConfig(parallelism), stormTopology);
        } else {
            StormSubmitter.submitTopology(FieldTemplate.FieldConstants.PCA.PCA, MlStormConfig.getDefaultMlStormConfig(parallelism), stormTopology);
        }
    }
}
