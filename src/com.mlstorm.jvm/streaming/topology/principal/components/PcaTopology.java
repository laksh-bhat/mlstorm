package topology.principal.components;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import bolt.ml.state.pca.create.WindowedPcaFactory;
import bolt.ml.state.pca.query.PrincipalComponentsAggregator;
import bolt.ml.state.pca.query.PrincipalComponentsQuery;
import bolt.ml.state.pca.update.PrincipalComponentUpdater;
import spout.sensor.SensorStreamingSpout;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchExecutor;
import storm.trident.state.StateFactory;
import utils.MlStormConfig;
import utils.fields.FieldTemplate;
import utils.fields.MlStormFieldTemplate;

/* license text */

public class PcaTopology {
    private static final int PCA_SAMPLE_SIZE = 100;
    private static final int PRINCIPAL_COMPONENTS = 5;

    private static StormTopology buildTopology(final int parallelism,
                                               final int pcaRowWidth,
                                               final int numPrincipalComponents,
                                               final FieldTemplate template) {
        final IRichSpout sensorSpout = new SensorStreamingSpout(template.getFields());
        final ITridentSpout batchSpout = new RichSpoutBatchExecutor(sensorSpout);
        final TridentTopology topology = new TridentTopology();
        final Stream sensorStream = topology.newStream(FieldTemplate.FieldConstants.PCA.PCA, batchSpout);
        final StateFactory pcaFactory = new WindowedPcaFactory(pcaRowWidth, numPrincipalComponents);

        final TridentState principalComponents =
                sensorStream
                        .partitionPersist(pcaFactory, new Fields(template.getKeyField(), template.getFeatureVectorField()), new PrincipalComponentUpdater())
                        .parallelismHint(parallelism);


        topology.newDRPCStream(FieldTemplate.FieldConstants.PCA.PCA_DRPC)
                .broadcast()
                .stateQuery(principalComponents, new Fields(FieldTemplate.FieldConstants.ARGS), new PrincipalComponentsQuery(), new Fields(FieldTemplate.FieldConstants.PCA.PCA_COMPONENTS))
                .project(new Fields(FieldTemplate.FieldConstants.PCA.PCA_COMPONENTS))
                .aggregate(new Fields(FieldTemplate.FieldConstants.PCA.PCA_COMPONENTS), new PrincipalComponentsAggregator(), new Fields(FieldTemplate.FieldConstants.PCA.PCA_EIGEN))
                .project(new Fields(FieldTemplate.FieldConstants.PCA.PCA_EIGEN));

        return topology.build();
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        final FieldTemplate fieldTemplate = new MlStormFieldTemplate();
        final int parallelism = args.length > 0 ? Integer.valueOf(args[0]) : 1;
        final StormTopology stormTopology = buildTopology(parallelism, PCA_SAMPLE_SIZE, PRINCIPAL_COMPONENTS, fieldTemplate);

        if (parallelism == 1) {
            final LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(FieldTemplate.FieldConstants.PCA.PCA, MlStormConfig.getDefaultMlStormConfig(parallelism), stormTopology);
        } else {
            StormSubmitter.submitTopology(FieldTemplate.FieldConstants.PCA.PCA, MlStormConfig.getDefaultMlStormConfig(parallelism), stormTopology);
        }
    }
}
