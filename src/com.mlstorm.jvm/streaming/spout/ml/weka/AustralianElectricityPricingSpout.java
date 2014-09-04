package spout.ml.weka;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import spout.ml.MlStormSpout;
import spout.utils.PeekableScanner;
import utils.MlStormFeatureVectorUtils;
import utils.fields.FieldTemplate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

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
public class AustralianElectricityPricingSpout implements MlStormSpout {

    private static final String FIELD_SEPARATOR = ",";
    private static final String POSITIVE_LABEL = "UP";
    private static final String NEGATIVE_LABEL = "DOWN";

    private final FieldTemplate fieldTemplate;
    private SpoutOutputCollector spoutOutputCollector;
    private PeekableScanner peekableScanner;
    private String fileName;
    private int tupleKey;

    public AustralianElectricityPricingSpout(String trainingDataFileName, FieldTemplate template) {
        this.fileName = trainingDataFileName;
        this.fieldTemplate = template;
        this.tupleKey = 0;
    }

    private int getTupleKey() {
        return tupleKey;
    }

    private void incrementTupleKey() {
        tupleKey++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(fieldTemplate.getFields()));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new Config();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        spoutOutputCollector = collector;
        Logger.getAnonymousLogger().log(Level.INFO, MessageFormat.format("Opening {0}", getClass().getCanonicalName()));
        try {
            peekableScanner = new PeekableScanner(new File(fileName));
            // MlStorm spouts must update feature vector length in the field template
            updateMlStormFieldTemplate(fieldTemplate, peekableScanner.peek().split(FIELD_SEPARATOR).length);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        peekableScanner.close();
    }

    @Override
    public void activate() {
        try {
            peekableScanner = new PeekableScanner(new File(fileName));
            int i = 0;
            while (i++ < getTupleKey()) {
                if (peekableScanner.hasNext()) {
                    peekableScanner.next();
                }
            }
        } catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void deactivate() {
        peekableScanner.close();
    }

    @Override
    public void nextTuple() {
        if (peekableScanner.hasNext()) {
            final String featureValuesText = peekableScanner.next();
            final double[] fv = getFeatureValues(featureValuesText);
            spoutOutputCollector.emit(MlStormFeatureVectorUtils.buildMlStormFeatureVector(getTupleKey(), fv));
            incrementTupleKey();
        }
    }

    public double[] getFeatureValues(String line) {
        String[] attrs = line.split(FIELD_SEPARATOR);
        double[] fv = new double[attrs.length];
        for (int i = 0; i < attrs.length; i++) {
            String attr = attrs[i];
            if (attr.equalsIgnoreCase(POSITIVE_LABEL)) {
                // positive represented by 1
                fv[i] = 1.0;
            } else if (attr.equalsIgnoreCase(NEGATIVE_LABEL)) {
                // negative represented by 0
                fv[i] = 0.0;
            } else {
                fv[i] = Double.valueOf(attr);
            }
        }
        return fv;
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
        peekableScanner.close();
    }

    @Override
    public void updateMlStormFieldTemplate(FieldTemplate template, int features) {
        this.fieldTemplate.setRuntimeFeatureCount(features);
    }

}
