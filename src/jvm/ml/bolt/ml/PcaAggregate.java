package bolt.ml;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 12/13/13
 * Time: 4:40 PM
 */
public class PcaAggregate implements IRichBolt {
    @Override
    public void prepare (final Map map, final TopologyContext topologyContext, final OutputCollector outputCollector) {

    }

    @Override
    public void execute (final Tuple tuple) {

    }

    @Override
    public void cleanup () {

    }

    @Override
    public void declareOutputFields (final OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration () {
        return null;
    }
}
