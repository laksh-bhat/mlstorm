package classifier.supervisedlearning.knn;


import classifier.Predictor;
import dataobject.FeatureVector;
import dataobject.Instance;
import utils.CommandLineUtilities;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class KNNPredictor extends Predictor {

    public static final int DEFAULT_KNN = 5;
    protected int kNearestNeighbors;
    protected List<Instance> dataset;
    protected Set<Integer> seenFeatures;

    public KNNPredictor() {
        dataset = new ArrayList<Instance>();
        seenFeatures = new HashSet<Integer>();
    }

    private void saveSeenFeaturesForPrediction(List<Instance> instances) {
        for (Instance instance : instances) {
            for (int feature : instance.getFeatureVector().getFeatureVectorKeys()) {
                seenFeatures.add(feature);
            }
        }
    }

    private void saveTrainingSetForPrediction(List<Instance> instances) {
        for (Instance instance : instances) {
            try {
                dataset.add((Instance) instance.clone());
            } catch (CloneNotSupportedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private void setKnn() {
        kNearestNeighbors = DEFAULT_KNN;
        if (CommandLineUtilities.hasArg("k_nn")) {
            kNearestNeighbors = CommandLineUtilities.getOptionValueAsInt("k_nn");
        }
    }

    @Override
    public void train(List<Instance> instances) {
        setKnn();
        saveTrainingSetForPrediction(instances);
        saveSeenFeaturesForPrediction(instances);
    }

    protected double computeDifferenceNorm(FeatureVector input, FeatureVector trainingVectorX) {
        double distance = 0;
        for (Integer feature : seenFeatures) {
            double inputFvVal = 0, trainingFvVal = 0;

            if (input.getFeatureVectorKeys().contains(feature)) {
                inputFvVal = input.get(feature);
            }
            if (trainingVectorX.getFeatureVectorKeys().contains(feature)) {
                trainingFvVal = trainingVectorX.get(feature);
            }
            distance += Math.pow((inputFvVal - trainingFvVal), 2);
        }
        return Math.sqrt(distance);
    }
}
