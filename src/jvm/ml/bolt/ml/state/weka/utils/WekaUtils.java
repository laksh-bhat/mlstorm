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

    public static FastVector getFeatureVectorForOnlineClustering(int noOfClusters, int noOfAttributes) {
        // Declare FAST VECTOR
        FastVector attributeInfo = new FastVector();

        // Declare FEATURES and add them to FEATURE VECTOR
        for (int i = 0; i < noOfAttributes; i++)
            attributeInfo.addElement(new Attribute(MessageFormat.format("feature-{0}", i)));

/*        FastVector clusters = new FastVector(noOfClusters);
        for (int i = 1; i <= noOfClusters; i++)
            clusters.addElement(MessageFormat.format("cluster-{0}", String.valueOf(i)));
        Attribute cluster = new Attribute("cluster", clusters);
        // last element in a FEATURE VECTOR is the category
        attributes.addElement(cluster);*/
        System.err.println("DEBUG: no. of attributes = " + attributeInfo.size());
        return attributeInfo;
    }

    public static FastVector getFeatureVectorForKmeansClustering(int noOfClusters, int noOfAttributes) {
        // Declare FAST VECTOR
        FastVector attributeInfo = new FastVector();

        // Declare FEATURES and add them to FEATURE VECTOR
        for (int i = 0; i < noOfAttributes; i++)
            attributeInfo.addElement(new Attribute(MessageFormat.format("feature-{0}", i)));

        FastVector clusters = new FastVector(noOfClusters);
        for (int i = 1; i <= noOfClusters; i++)
            clusters.addElement(MessageFormat.format("cluster-{0}", String.valueOf(i)));
        Attribute cluster = new Attribute("cluster", clusters);
        // last element in a FEATURE VECTOR is the category
        attributeInfo.addElement(cluster);

        System.err.println("DEBUG: no. of attributes = " + attributeInfo.size());
        return attributeInfo;
    }
}
