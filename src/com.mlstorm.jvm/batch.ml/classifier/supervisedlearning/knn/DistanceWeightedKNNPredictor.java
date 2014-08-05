package classifier.supervisedlearning.knn;

import dataobject.FeatureVector;
import dataobject.Instance;
import dataobject.label.Label;
import dataobject.label.RegressionLabel;

import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;


public class DistanceWeightedKNNPredictor extends KNNPredictor {

    private double computePredictionValue(
            Instance instance, double prediction, HashMap<Double, Instance>
            kNeighbors, double sumOfSimilarity
    ) {
        for (Instance nearestNeighborInstance : kNeighbors.values()) {
            Label nearestNeighbor = nearestNeighborInstance.getLabel();
            double lambda = computeSimilarity(instance.getFeatureVector(), nearestNeighborInstance.getFeatureVector())
                    / sumOfSimilarity;
            prediction += lambda * nearestNeighbor.getLabelValue();
        }
        return prediction;
    }

    private double computeSimilarityByCreatingSubsetOfKNearestNeighbors(
            int k, SortedMap<Double, Instance>
            neighborDistanceWithPrediction, Instance instance,
            HashMap<Double, Instance> kNeighbors) {
        double summationSimilarity = 0;
        for (int i = 0; i < k; i++) {
            final Instance nearestNeighborInstance = neighborDistanceWithPrediction.get(neighborDistanceWithPrediction.firstKey());
            kNeighbors.put(neighborDistanceWithPrediction.firstKey(), nearestNeighborInstance);
            summationSimilarity += computeSimilarity(instance.getFeatureVector(), nearestNeighborInstance.getFeatureVector());
            neighborDistanceWithPrediction.remove(neighborDistanceWithPrediction.firstKey());
        }
        return summationSimilarity;
    }

    double computeSimilarity(FeatureVector fv1, FeatureVector fv2) {
        return 1 / (1 + computeDifferenceNorm(fv1, fv2));
    }

    @Override
    public Label predict(Instance instance) {
        SortedMap<Double, Instance> neighborDistanceWithPrediction = new TreeMap<Double, Instance>();
        for (Instance trainingInstance : dataset) {
            neighborDistanceWithPrediction.put(computeDifferenceNorm(instance.getFeatureVector(), trainingInstance.getFeatureVector()), trainingInstance);
        }
        return predictLabel(kNearestNeighbors, neighborDistanceWithPrediction, instance);
    }

    protected Label predictLabel(int k, SortedMap<Double, Instance> neighborDistanceWithPrediction, Instance instance) {
        double prediction = 0;
        HashMap<Double, Instance> kNeighbors = new HashMap<Double, Instance>();

        double sumOfSimilarity = computeSimilarityByCreatingSubsetOfKNearestNeighbors(k, neighborDistanceWithPrediction, instance, kNeighbors);
        prediction = computePredictionValue(instance, prediction, kNeighbors, sumOfSimilarity);

        return new RegressionLabel(prediction);
    }

}
