package spout.mddb;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.LoggerFactory;
import utils.SpoutUtils;

import java.io.File;
import java.text.MessageFormat;
import java.util.*;

/**
 * Created by lbhat@DaMSl on 12/22/13.
 * <p/>
 * Copyright {2013} {Lakshmisha Bhat <laksh85@gmail.com>}
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

public class MddbFeatureExtractorSpout implements IRichSpout {

    private final String[] fields;
    private final String folder;
    private final org.slf4j.Logger logger;
    private Queue<String> featureFiles;

    private int messageId;
    private int taskId;
    private String previous;
    private transient Scanner scanner;
    private SpoutOutputCollector collector;

    /**
     * Feature streaming spout for mddb data-set
     *
     * @param directory          the directory where you read the feature files from
     * @param initialStormFields
     */
    public MddbFeatureExtractorSpout(String directory, String[] initialStormFields) {
        this.fields = initialStormFields;
        this.folder = directory;

        logger = LoggerFactory.getLogger(MddbFeatureExtractorSpout.class);
    }

    private Scanner getScanner(final String filePath) {
        if (scanner != null) scanner.close();
        if (filePath != null) return new Scanner(filePath);
        else return null;
    }

    /**
     * Declare the output schema for all the streams of this topology.
     *
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(fields));
    }

    /**
     * Declare configuration specific to this component. Only a subset of the "topology.*" configs can
     * be overridden. The component configuration can be further overridden when constructing the
     * topology using {@link backtype.storm.topology.TopologyBuilder}
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new Config();
    }

    /**
     * Called when a task for this component is initialized within a worker on the cluster.
     * It provides the spout with the environment in which the spout executes.
     * <p/>
     * <p>This includes the:</p>
     *
     * @param conf      The Storm configuration for this spout. This is the configuration provided to the topology merged in with cluster configuration on this machine.
     * @param context   This object can be used to pythonDictToJava information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
     * @param collector The collector is used to emit tuples from this spout. Tuples can be emitted at any time, including the open and close methods. The collector is thread-safe and should be saved as an instance variable of this spout object.
     */
    @Override
    public void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        System.err.println("DEBUG: Opening MddbFeatureExtractorSpout... ");
        this.collector = collector;
        this.taskId = context.getThisTaskId();
        featureFiles = new ArrayDeque<String>();
        SpoutUtils.listFilesForFolder(new File(folder), featureFiles);
        scanner = moveSpoutForward();
    }

    /**
     * Called when an ISpout is going to be shutdown. There is no guarentee that close
     * will be called, because the supervisor kill -9's worker processes on the cluster.
     * <p/>
     * <p>The one context where close is guaranteed to be called is a topology is
     * killed when running Storm in local mode.</p>
     */
    @Override
    public void close() {
        if (scanner != null) scanner.close();
    }

    /**
     * Called when a spout has been activated out of a deactivated mode.
     * nextTuple will be called on this spout soon. A spout can become activated
     * after having been deactivated when the topology is manipulated using the
     * `storm` client.
     */
    @Override
    public void activate() {
        // no op for now
    }

    /**
     * Called when a spout has been deactivated. nextTuple will not be called while
     * a spout is deactivated. The spout may or may not be reactivated in the future.
     */
    @Override
    public void deactivate() {
        // again, no op for now
    }

    /**
     * When this method is called, Storm is requesting that the Spout emit tuples to the
     * output collector. This method should be non-blocking, so if the Spout has no tuples
     * to emit, this method should return. nextTuple, ack, and fail are all called in a tight
     * loop in a single thread in the spout task. When there are no tuples to emit, it is courteous
     * to have nextTuple sleep for a short amount of time (like a single millisecond)
     * so as not to waste too much CPU.
     */
    @Override
    public synchronized void nextTuple() {
        if (scanner == null) {
            System.err.println(MessageFormat.format("No more featureVectorsInWindow. Visit {0} later", taskId));
        } else {
            try {
                if (scanner.hasNextLine()) previous = scanner.nextLine();
                List<Map<String, List<Double>>> dict = SpoutUtils.pythonDictToJava(previous);
                for (Map<String, List<Double>> map : dict) {
                    Double[] features = map.get("chi1").toArray(new Double[0]);
                    Double[] moreFeatures = map.get("chi2").toArray(new Double[0]);
                    Double[] both = (Double[]) ArrayUtils.addAll(features, moreFeatures);
                    collector.emit(new Values(messageId++, ArrayUtils.toPrimitive(both)));
                }
                scanner = moveSpoutForward();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Storm has determined that the tuple emitted by this spout with the msgId identifier
     * has been fully processed. Typically, an implementation of this method will take that
     * message off the queue and prevent it from being replayed.
     *
     * @param msgId
     */
    @Override
    public void ack(final Object msgId) {
        //scanner = moveSpoutForward();
    }

    private synchronized Scanner moveSpoutForward() {
        System.err.println("DEBUG: Moving spout forward...");
        return getScanner(featureFiles.size() > 0 ? featureFiles.remove() : null);
    }

    /**
     * The tuple emitted by this spout with the msgId identifier has failed to be
     * fully processed. Typically, an implementation of this method will put that
     * message back on the queue to be replayed at a later time.
     *
     * @param msgId
     */
    @Override
    public void fail(final Object msgId) {
        // since this failed, we are not going to forward the scanner to the next batch of featureVectorsInWindow
        // so no-op
    }
}
