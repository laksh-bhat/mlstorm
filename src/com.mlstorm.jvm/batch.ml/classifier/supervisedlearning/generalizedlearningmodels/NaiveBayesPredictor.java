package classifier.supervisedlearning.generalizedlearningmodels;

import classifier.Predictor;
import dataobject.Instance;
import dataobject.label.Label;
import utils.CommandLineUtilities;

import java.util.*;

public class NaiveBayesPredictor extends Predictor {
    private int noOfFeatures;
    private double lambda;
    private Set<Integer> features;
    private HashMap<Label, Integer> countOfLabels;
    private List<Instance> trainingInstances;
    private HashMap<Label, Double> probabilityOfLabels;
    private HashMap<Label, Double> likelihoodOfLabelGivenFeatures;
    private HashMap<Integer, Double> meanOfFeatures;
    private HashMap<Integer, Boolean> featureTypes;
    private HashMap<Label, HashMap<Integer, Double>> labelsFeatureProbability;
    private HashMap<Label/*outputLabel*/,
            HashMap<Integer /*columnIndex*/,
                    HashMap<Double /*Value*/, Integer /*frequency*/>>> labelToFeatureValueCount;  // P(Xi|Y=yj) is got from this map


    public NaiveBayesPredictor() {
        lambda = 0.0;
        features = new HashSet<Integer>();
        featureTypes = new HashMap<Integer, Boolean>();
        countOfLabels = new HashMap<Label, Integer>();
        meanOfFeatures = new HashMap<Integer, Double>();
        trainingInstances = new ArrayList<Instance>();
        probabilityOfLabels = new HashMap<Label, Double>();
        labelsFeatureProbability = new HashMap<Label, HashMap<Integer, Double>>();
        likelihoodOfLabelGivenFeatures = new HashMap<Label, Double>();
        labelToFeatureValueCount = new HashMap<Label, HashMap<Integer, HashMap<Double, Integer>>>();
    }

    private void cleanup() {
        labelToFeatureValueCount.clear();
    }

    private void cloneTrainingInstances(List<Instance> instances) {
        for (Instance instance : instances) {
            try {
                trainingInstances.add((Instance) instance.clone());
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        }
    }

    private void computeMeanOfAllFeatures(
            List<Instance> instances,
            HashMap<Integer, Double> sumsOfFeatures,
            HashMap<Integer, Double> meanOfFeatures
    ) {
        for (Instance instance : instances) {
            for (Map.Entry<Integer, Double> fvCell : instance.getFeatureVector().getEntrySet()) {
                Double value = sumsOfFeatures.get(fvCell.getKey());
                if (value == null) {
                    value = 0.0;
                }
                sumsOfFeatures.put(fvCell.getKey(), value + fvCell.getValue());
            }
        }
        for (Map.Entry<Integer, Double> featureSum : sumsOfFeatures.entrySet()) {
            meanOfFeatures.put(featureSum.getKey(), featureSum.getValue() / countInstancesThatContainFeature(
                    instances, featureSum.getKey()));
        }
    }

    private void computeMeanOfFeatures(List<Instance> instances) {
        HashMap<Integer, Double> sumsOfFeatures = new HashMap<Integer, Double>();
        computeMeanOfAllFeatures(instances, sumsOfFeatures, meanOfFeatures);
    }

    private void computeProbabilityOfAllLabels(
            List<Instance> instances,
            HashMap<Label, Double> probabilityOfLabels,
            double lambda) {
        for (Map.Entry<Label, Integer> labelAndCount : countOfLabels.entrySet()) {
            probabilityOfLabels.put(labelAndCount.getKey(),
                    (labelAndCount.getValue() + lambda) / (instances.size() + lambda * countOfLabels.size()));
        }
    }

    private void computeProbabilityOfFeatureGivenLabels(
            HashMap<Label, HashMap<Integer, Double>> labelsFeatureProbability,
            double lambda) {
        for (Label label : labelToFeatureValueCount.keySet()) {
            if (!labelsFeatureProbability.containsKey(label)) {
                labelsFeatureProbability.put(label, new HashMap<Integer, Double>());
            }

            for (int feature : labelToFeatureValueCount.get(label).keySet()) {
                HashMap<Double, Integer> featureValueCount = labelToFeatureValueCount.get(label).get(feature);
                int countFiringFeatures = 0;
                if (featureValueCount.containsKey(1.0)) {
                    countFiringFeatures = featureValueCount.get(1.0);
                }

                double pOfXGivenY = (countFiringFeatures + lambda) / (countOfLabels.get(label) + lambda);

                labelsFeatureProbability.get(label).put(feature, pOfXGivenY);
            }
        }
    }

    private double computeSummationOfLogConditionalProbabilitiesOfAllFeatures(Label label, double logSumOfProbabilitiesForThisFeature, int feature, double value) {
        double probabilityOfFeatureGivenLabel = getProbabilityOfXConditionedOnY(label, feature, value);
        if (probabilityOfFeatureGivenLabel == 0) {
            probabilityOfFeatureGivenLabel = getLambda() / (countOfLabels.get(label) + getLambda());
        }
        logSumOfProbabilitiesForThisFeature += Math.log(probabilityOfFeatureGivenLabel);
        return logSumOfProbabilitiesForThisFeature;
    }

    private void convertContinuousFeaturesIntoBinaryBySplitting(
            List<Instance> instances,
            HashMap<Integer, Double> meanOfFeatures,
            HashMap<Integer, Boolean> binaryFeatures) {
        for (int i = 0; i < instances.size(); i++) {
            Instance instance = instances.get(i);
            Instance trainingInstance = trainingInstances.get(i);

            for (Integer feature : instance.getFeatureVector().getFeatureVectorKeys()) {
                if (!(binaryFeatures.get(feature) == null ? false : binaryFeatures.get(feature))) {  // needs splitting
                    if (instance.getFeatureVector().get(feature) >= meanOfFeatures.get(feature)) {
                        trainingInstance.getFeatureVector().add(feature, 1);
                        trainingInstance.getFeatureVector().add(feature + noOfFeatures, 0);
                    } else {
                        trainingInstance.getFeatureVector().add(feature, 0);
                        trainingInstance.getFeatureVector().add(feature + noOfFeatures, 1);
                    }
                } else {
                    trainingInstance.getFeatureVector().add(feature + noOfFeatures, 0);
                }
            }
        }
    }

    private void cookFrequencyOfFeatureValues(List<Instance> instances) {
        for (Instance instance : instances) {
            for (int feature : instance.getFeatureVector().getFeatureVectorKeys()) {
                Double value = instance.getFeatureVector().get(feature);
                HashMap<Integer, HashMap<Double, Integer>> featureValueCount;

                if (labelToFeatureValueCount.containsKey(instance.getLabel())) {
                    featureValueCount = labelToFeatureValueCount.get(instance.getLabel());
                } else {
                    featureValueCount = new HashMap<Integer, HashMap<Double, Integer>>();
                    labelToFeatureValueCount.put(instance.getLabel(), featureValueCount);
                }

                HashMap<Double, Integer> countMap;
                if (featureValueCount.containsKey(feature)) {
                    countMap = featureValueCount.get(feature);
                    int countOfValue = countMap.containsKey(value) ? countMap.get(value) : 0;
                    countOfValue++;
                    countMap.put(value, countOfValue);
                } else {
                    countMap = new HashMap<Double, Integer>();
                    countMap.put(value, 1);
                }

                featureValueCount.put(feature, countMap);
            }
        }
    }

    private int countInstancesThatContainFeature(List<Instance> instances, int feature) {
        int count = 0;
        for (Instance instance : instances) {
            if (instance.getFeatureVector().getFeatureVectorKeys().contains(feature)) {
                count++;
            }
        }
        return count;
    }

    private Label getLabelWithMaxLikelihood(HashMap<Label, Double> likelihoodOfLabelGivenFeatures) {
        Label predictedL = null;
        double maxLikelihood = Double.NEGATIVE_INFINITY;
        for (Map.Entry<Label, Double> labelAndItsLikelihood : likelihoodOfLabelGivenFeatures.entrySet()) {
            if (labelAndItsLikelihood.getValue() > maxLikelihood) {
                maxLikelihood = labelAndItsLikelihood.getValue();
                predictedL = labelAndItsLikelihood.getKey();
            }
        }
        return predictedL;
    }

    private double getLambda() {
        if (lambda > 0) {
            return lambda;
        }

        lambda = 1.0;
        if (CommandLineUtilities.hasArg("lambda")) {
            lambda = CommandLineUtilities.getOptionValueAsFloat("lambda");
        }
        return lambda;
    }

    private Double getProbabilityOfXConditionedOnY(
            Label label,
            Integer feature, double value) {
        double probabilityOfFeatureGivenLabel = 0;

        if (featureTypes.get(feature) || value > meanOfFeatures.get(feature)) {
            if (labelsFeatureProbability.get(label).containsKey(feature)) {
                probabilityOfFeatureGivenLabel = labelsFeatureProbability.get(label).get(feature);
            }
        } else {
            if (labelsFeatureProbability.get(label).containsKey(feature + noOfFeatures)) {
                probabilityOfFeatureGivenLabel += labelsFeatureProbability.get(label).get(feature + noOfFeatures);
            }
        }

        return probabilityOfFeatureGivenLabel;
    }

    private int getTotalNoOfFeatures(List<Instance> instances) {
        int maxIndex = 0;
        for (Instance instance : instances) {
            for (Integer feature : instance.getFeatureVector().getFeatureVectorKeys()) {
                features.add(feature);
                if (feature > maxIndex) {
                    maxIndex = feature;
                }
            }
        }
        return maxIndex;
    }

    private void makeBinaryOrContinuousFeatureClassification(List<Instance> instances) {
        markAllFeaturesAsBinary();

        for (Instance instance : instances) {
            for (Integer feature : instance.getFeatureVector().getFeatureVectorKeys()) {
                if (!featureTypes.get(feature)) {
                    continue; // Already set, nothing to do
                }

                double featureValue = instance.getFeatureVector().get(feature);
                if (featureValue != 0.0 && featureValue != 1.0) {
                    featureTypes.put(feature, false);
                }
            }
        }
    }

    private void markAllFeaturesAsBinary() {
        for (int i = 1; i <= noOfFeatures; i++) {
            featureTypes.put(i, true);
        }
    }

    @Override
    public void train(List<Instance> instances) {
        double lambda = getLambda();
        cloneTrainingInstances(instances);
        noOfFeatures = getTotalNoOfFeatures(trainingInstances);

        makeBinaryOrContinuousFeatureClassification(trainingInstances);
        computeMeanOfFeatures(trainingInstances);
        convertContinuousFeaturesIntoBinaryBySplitting(instances, meanOfFeatures, featureTypes);

        cookFrequencyOfFeatureValues(trainingInstances);
        countLabelsAndCacheIt(trainingInstances);

        computeProbabilityOfAllLabels(instances, probabilityOfLabels, lambda);
        computeProbabilityOfFeatureGivenLabels(labelsFeatureProbability, lambda);

        cleanup();
    }

    @Override
    public Label predict(Instance instance) {
        for (Label label : labelsFeatureProbability.keySet()) {
            double logSumOfProbabilitiesForFeature = 0;
            double logProbabilityOfThisLabel = Math.log(probabilityOfLabels.get(label));

            for (int feature : instance.getFeatureVector().getFeatureVectorKeys()) {
                double value = instance.getFeatureVector().get(feature);
                if (!features.contains(feature)) {
                    continue;
                }

                logSumOfProbabilitiesForFeature =
                        computeSummationOfLogConditionalProbabilitiesOfAllFeatures
                                (label, logSumOfProbabilitiesForFeature, feature, value);
            }

            likelihoodOfLabelGivenFeatures.put(label, logSumOfProbabilitiesForFeature + logProbabilityOfThisLabel);
        }
        return getLabelWithMaxLikelihood(likelihoodOfLabelGivenFeatures);
    }

    public void countLabelsAndCacheIt(List<Instance> instances) {
        for (Instance instance : instances) {
            countOfLabels.put(instance.getLabel(), 0);
        }

        for (Instance instance : instances) {
            countOfLabels.put(instance.getLabel(), countOfLabels.get(instance.getLabel()) + 1);
        }
    }
}
