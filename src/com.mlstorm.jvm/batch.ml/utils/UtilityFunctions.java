package utils;

import dataobject.FeatureVector;
import dataobject.Instance;

import java.util.List;

public class UtilityFunctions {
    public static double computeL2Norm(FeatureVector fv1, FeatureVector fv2) {
        double squareNorm = 0;

        for (Integer feature : fv1.getFeatureVectorKeys()) {
            if (fv2.getFeatureVectorKeys().contains(feature)) {
                squareNorm += Math.pow(fv1.get(feature) - fv2.get(feature), 2);
            } else {
                squareNorm += Math.pow(fv1.get(feature), 2);
            }
        }

        for (Integer feature : fv2.getFeatureVectorKeys()) {
            if (!fv1.getFeatureVectorKeys().contains(feature)) {
                squareNorm += Math.pow(fv2.get(feature), 2);
            }
        }
        return Math.sqrt(squareNorm);
    }

    public static double computeL2Norm(FeatureVector fv) {
        double squareNorm = 0;

        for (Integer feature : fv.getFeatureVectorKeys()) {
            squareNorm += Math.pow(fv.get(feature), 2);
        }

        return Math.sqrt(squareNorm);
    }

    public static int getNumberOfFeatures(List<Instance> instances) {
        int maxIndex = 0;
        for (Instance instance : instances) {
            for (Integer feature : instance.getFeatureVector().getFeatureVectorKeys()) {
                if (feature > maxIndex) {
                    maxIndex = feature;
                }
            }
        }
        return maxIndex;
    }
}
