package spout.text;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

/* license text */

@SuppressWarnings({"serial", "rawtypes"})
public class FileStreamingSpout implements IRichSpout {
    SpoutOutputCollector _collector;
    private Scanner scanner;
    private String fileName;
    private String[] fields;

    public FileStreamingSpout(String fileName, String[] fields) {
        this.fileName = fileName;
        this.fields = fields;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(fields));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new Config();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        System.err.println("Open Spout instance");
        try {
            scanner = new Scanner(new File(fileName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        scanner.close();
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void nextTuple() {
        _collector.emit(new Values(scanner.nextLine()));
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
        scanner.close();
    }
}
