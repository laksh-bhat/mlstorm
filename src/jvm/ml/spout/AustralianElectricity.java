package spout;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by lbhat@DaMSl on 4/17/14.
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
@SuppressWarnings({"serial", "rawtypes"})
public class AustralianElectricity implements IRichSpout {

        SpoutOutputCollector _collector;
        private Scanner scanner;
        private String fileName;
        private String[] fields;
        private int messageId;

        public AustralianElectricity(String fileName, String[] fields) {
            this.fileName = fileName;
            this.fields = fields;
            messageId = 0;
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
            System.err.println(MessageFormat.format("Open Spout instance - {0}", this.getClass().toString()));
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
            try {
                scanner = new Scanner(new File(fileName));
                int i = 0;
                while (i++ < messageId) if (scanner.hasNextLine()) scanner.nextLine();
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void deactivate() {
            scanner.close();
        }

        @Override
        public void nextTuple() {
            if (scanner.hasNextLine()){
                String line = scanner.nextLine();
                String[] attrs = line.split(",");
                Double[] fv = new Double[attrs.length];
                for (int i = 0; i < attrs.length; i++) {
                    String attr = attrs[i];
                    if (attr.equalsIgnoreCase("UP")) fv[i] = 1.0;
                    else if (attr.equalsIgnoreCase("DOWN")) fv [i] = 0.0;
                    else fv[i] = Double.valueOf(attr);
                }
                _collector.emit(new Values(messageId++, fv));
            }
        }

        @Override
        public void ack(Object msgId) {
        }

        @Override
        public void fail(Object msgId) {
            scanner.close();
        }
    }
