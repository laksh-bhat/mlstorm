package spout;

import backtype.storm.topology.IRichSpout;
import utils.fields.FieldTemplate;

/**
 * Created by lakshmisha.bhat on 7/28/14.
 */
public interface MlStormSpout extends IRichSpout {
    // This must be called before the spout emits any tuples.
    void updateMlStormFieldTemplate(FieldTemplate template, int numFeatures);
}
