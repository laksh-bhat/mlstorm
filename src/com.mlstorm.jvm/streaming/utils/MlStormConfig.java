package utils;

import backtype.storm.Config;
import com.google.common.collect.Lists;

/**
 * Created by lakshmisha.bhat on 7/29/14.
 */
public class MlStormConfig {
    public static Config getDefaultMlStormConfig(int numWorkers) {
        final Config conf = new Config();
        conf.setNumAckers(numWorkers);
        conf.setNumWorkers(numWorkers);
        conf.setMaxSpoutPending(8); // This is critical; if you don't set this, it's likely that you'll run out of memory and storm will throw wierd errors

        conf.put("topology.spout.max.batch.size", 1000 /* x1000 i.e. every tuple has 1000 feature vectors*/);
        conf.put("topology.trident.batch.emit.interval.millis", 1000);
        // These are the DRPC servers our topology is going to use. So clients must know about this.
        // Its hard-coded here so that I could play with it
        // I'm using a 5 node cluster (1 nimbus, 4 nodes acting as both supervisors and drpc servers)
        conf.put(Config.DRPC_SERVERS, Lists.newArrayList("qp-hd3", "qp-hd4", "qp-hd5", "qp-hd6"));
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        conf.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 30);
        return conf;
    }

    public static Config getEnsembleMlStormConfig(int numWorkers) {
        final Config conf = new Config();
        conf.setNumAckers(numWorkers);
        conf.setNumWorkers(numWorkers);
        conf.setMaxSpoutPending(20); // This is critical; if you don't set this, it's likely that you'll run out of memory and storm will throw wierd errors

        conf.put("topology.spout.max.batch.size", 1000 /* x1000 i.e. every tuple has 1000 feature vectors*/);
        conf.put("topology.trident.batch.emit.interval.millis", 1000);
        // These are the DRPC servers our topology is going to use. So clients must know about this.
        // Its hard-coded here so that I could play with it
        // I'm using a 5 node cluster (1 nimbus, 4 nodes acting as both supervisors and drpc servers)
        conf.put(Config.DRPC_SERVERS, Lists.newArrayList("qp-hd3", "qp-hd4", "qp-hd5", "qp-hd6"));
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        conf.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 60);
        return conf;
    }

    public static Config getMddbStormConfig(int numWorkers) {
        Config conf = new Config();
        conf.setNumAckers(numWorkers);
        conf.setNumWorkers(numWorkers);
        conf.setMaxSpoutPending(10); // This is critical; if you don't set this, it's likely that you'll run out of memory and storm will throw wierd errors
        conf.put("topology.spout.max.batch.size", 1 /* x1000 i.e. every tuple has 1000 feature vectors*/);
        conf.put("topology.trident.batch.emit.interval.millis", 500);
        // These are the DRPC servers our topology is going to use. So clients must know about this.
        // Its hard-coded here so that I could play with it
        // I'm using a 5 node cluster (1 nimbus, 4 nodes acting as both supervisors and drpc servers)
        conf.put(Config.DRPC_SERVERS, Lists.newArrayList("qp-hd3", "qp-hd4", "qp-hd5", "qp-hd6"));
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        conf.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 30);
        return conf;
    }
}
