package classifier.supervisedlearning.ensembles;

import classifier.Predictor;
import classifier.supervisedlearning.generalizedlearningmodels.PerceptronPredictor;
import dataobject.FeatureVector;
import dataobject.Instance;
import dataobject.label.ClassificationLabel;
import dataobject.label.Label;
import utils.CommandLineUtilities;

import java.util.HashMap;
import java.util.List;


public abstract class EnsemblePredictor extends Predictor {
    protected HashMap<Integer, Double> weights;
    protected HashMap<Integer, HashMap<Integer, Double>> classifierWeightVector;

    public EnsemblePredictor() {
        weights = new HashMap<Integer, Double>();
        classifierWeightVector = new HashMap<Integer, HashMap<Integer, Double>>();
        initializeWeights();
    }

    protected abstract List<Instance> getInstancesForEnsembleTraining(
            int classifier,
            int k,
            List<Instance> instances);

    private double getLinearCombinationWDotX(
            FeatureVector fv,
            HashMap<Integer, Double> weightVector) {
        double wDotX = 0;
        for (int feature : fv.getFeatureVectorKeys()) {
            if (weightVector.containsKey(feature)) {
                wDotX += weightVector.get(feature) * fv.get(feature);
            }
        }
        return wDotX;
    }

    private void initializeWeights() {
        int k = getNoOfClassifiers();
        for (int i = 0; i < k; i++) {
            weights.put(i, 0.0);
        }
    }

    protected int getTrainingIterations() {
        int ensemble_training_iterations = 5;
        if (CommandLineUtilities.hasArg("ensemble_training_iterations")) {
            ensemble_training_iterations = CommandLineUtilities.getOptionValueAsInt("ensemble_training_iterations");
        }
        return ensemble_training_iterations;
    }

    protected double getEnsembleLearningRate() {
        double ensemble_learning_rate = 0.1;
        if (CommandLineUtilities.hasArg("ensemble_learning_rate")) {
            ensemble_learning_rate = CommandLineUtilities.getOptionValueAsFloat("ensemble_learning_rate");
        }
        return ensemble_learning_rate;
    }

    protected int getNoOfClassifiers() {
        int k_ensemble = 5;
        if (CommandLineUtilities.hasArg("k_ensemble")) {
            k_ensemble = CommandLineUtilities.getOptionValueAsInt("k_ensemble");
        }
        return k_ensemble;
    }

    protected double g(double z) {
        return z / Math.sqrt(1 + Math.pow(z, 2));
    }

    protected int h(double z) {
        if (z >= 0) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public Label predict(Instance instance) {
        return predictLabel(classifierWeightVector.size(), instance);
    }

    protected Label predictLabel(int noOfClassifiers, Instance instanceX) {
        double summationProductOfMuAndG = 0;
        for (int classifierIndex = 0; classifierIndex < noOfClassifiers; classifierIndex++) {
            // linear combination
            double wDotX = getLinearCombinationWDotX(instanceX.getFeatureVector(), classifierWeightVector.get(classifierIndex));
            summationProductOfMuAndG += weights.get(classifierIndex) * g(wDotX);
        }
        return new ClassificationLabel(h(summationProductOfMuAndG));
    }

    protected void updateWeightOfClassifier(int noOfClassifiers, Instance instance) {
        Label yi = instance.getLabel();
        double ensembleLearningRate = getEnsembleLearningRate();

        for (int classifierIndex = 0; classifierIndex < noOfClassifiers; classifierIndex++) {
            double wDotX = getLinearCombinationWDotX(instance.getFeatureVector(),
                    classifierWeightVector.get(classifierIndex));

            if (yi.getLabelValue() == 1) {
                weights.put(classifierIndex, weights.get(classifierIndex) + ensembleLearningRate * g(wDotX));
            } else if (yi.getLabelValue() == 0) {
                weights.put(classifierIndex, weights.get(classifierIndex) - ensembleLearningRate * g(wDotX));
            } else {
                System.out.println("this should never happen");
            }
        }
    }

    protected void trainClassifiers(List<Instance> instances, int k) {
        for (int classifier = 0; classifier < k; classifier++) {
            List<Instance> instanceBag = getInstancesForEnsembleTraining(classifier, k, instances);
            PerceptronPredictor perceptron = new PerceptronPredictor();
            perceptron.setLearningRateEeta(1.0);
            perceptron.setNoOfLearningIterationsI(5);
            perceptron.train(instanceBag);
            classifierWeightVector.put(classifier, perceptron.getWeightVectorW());
        }
    }
}
