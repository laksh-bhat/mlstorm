package bdconsistency.spouts;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings ({"serial", "rawtypes"})
public class NonTransactionalFileStreamingSpout implements IRichSpout {
    SpoutOutputCollector _collector;
    private Scanner scanner;
    private String  fileName;
    private String  fieldName;

    public NonTransactionalFileStreamingSpout (String fileName, String fieldName) {
        this.fileName = fileName;
        this.fieldName = fieldName;
    }

    @Override
    public void declareOutputFields (OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(fieldName));
    }

    @Override
    public Map<String, Object> getComponentConfiguration () {
        return new Config();
    }

    @Override
    public void open (Map conf, TopologyContext context, SpoutOutputCollector collector) {
        System.err.println("Debug: Opening NonTransactionalFileStreamingSpout Instance...");
        _collector = collector;
        try {
            scanner = new Scanner(new File(fileName));
        } catch ( IOException e ) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close () {
        scanner.close();
    }

    @Override
    public void activate () {
        if (scanner != null)
            scanner.close();
        scanner = new Scanner(fileName);
    }

    @Override
    public void deactivate () {
        scanner.close();
    }

    @Override
    public void nextTuple () {
        /*try {*/
        if (scanner.hasNextLine())
            _collector.emit(new Values(scanner.nextLine()));

            /*else {
                System.err.println("Debug: No More Tuples...");
                Thread.sleep(60000);
            }
        } catch ( InterruptedException ignore ) {}*/
    }

    @Override
    public void ack (Object msgId) {}

    @Override
    public void fail (Object msgId) {}
}
