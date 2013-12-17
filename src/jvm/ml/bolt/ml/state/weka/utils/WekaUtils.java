package bolt.ml.state.weka.utils;

import weka.core.Attribute;
import weka.core.FastVector;

import java.text.MessageFormat;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/16/13
 * Time: 8:37 PM
 */
public class WekaUtils {
    //TODO correct this
    public static FastVector getFeatureVectorForClustering (int noOfClusters, int noOfAttributes) {
        // Declare FEATURE VECTOR
        FastVector attributes = new FastVector(noOfAttributes);
        // Declare FEATURES and add them to FEATURE VECTOR
        for (int i = 1; i < noOfAttributes; i++)
            attributes.addElement(new Attribute(MessageFormat.format("feature-{0}", i)));

        FastVector clusters = new FastVector(noOfClusters);
        for (int i = 1; i <= noOfClusters; i++)
            clusters.addElement(MessageFormat.format("cluster-{0}", String.valueOf(i)));
        Attribute cluster = new Attribute("cluster", clusters);
        // last element in a FEATURE VECTOR is the category
        attributes.addElement(cluster);
        return attributes;
    }
}
