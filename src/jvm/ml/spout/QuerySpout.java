package bdconsistency.spouts;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 10/7/13
 * Time: 8:51 AM
 */
public class QuerySpout implements IRichSpout {
    SpoutOutputCollector _collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("query"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        int tickFrequencyInSeconds = 10;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void close() {}

    @Override
    public void activate() {}

    @Override
    public void deactivate() {}

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) {}
        _collector.emit(new Values("__query__"));
    }

    @Override
    public void ack(Object msgId) {}

    @Override
    public void fail(Object msgId) {}
}
