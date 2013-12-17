package topology.weka;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import spout.text.FileStreamingSpout;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.QueryFunction;
import storm.trident.state.StateFactory;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/16/13
 * Time: 7:31 PM
 */
public class WekaLearningBaseTopology {
    private static StormTopology buildTopology (final int parallelism,
                                                final String filename,
                                                final String[] fields,
                                                final BaseStateUpdater stateUpdater,
                                                final StateFactory stateFactory,
                                                final QueryFunction queryFunction,
                                                final String drpcFunction)
    {
        TridentTopology topology = new TridentTopology();
        IRichSpout features = new FileStreamingSpout(filename, fields);
        Stream featuresStream = topology.newStream("featureVector", features);

        TridentState state =
                featuresStream.partitionBy(new Fields("featureVector"))
                              .partitionPersist(stateFactory, new Fields("featureVector"), stateUpdater)
                              .parallelismHint(parallelism);


        Stream stateStream
                = topology.newDRPCStream(drpcFunction)
                          .broadcast()
                          .stateQuery(state, new Fields("args"), queryFunction, new Fields("result"));

        return topology.build();
    }

}
