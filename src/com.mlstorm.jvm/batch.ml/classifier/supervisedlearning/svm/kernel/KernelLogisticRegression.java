package classifier.supervisedlearning.svm.kernel;

import classifier.Predictor;
import dataobject.FeatureVector;
import dataobject.Instance;
import dataobject.label.ClassificationLabel;
import dataobject.label.Label;
import utils.CommandLineUtilities;

import java.util.List;

public abstract class KernelLogisticRegression extends Predictor {

    protected int iterations;
    protected double learningRate;
    protected double[] alphaVector;
    protected double[][] gramMatrix;
    protected List<Instance> trainingInstances;
    protected double[] preCachedAlphaDotGramMatrix;


    public KernelLogisticRegression() {
        iterations = 5;
        if (CommandLineUtilities.hasArg("gradient_ascent_training_iterations")) {
            iterations =
                    CommandLineUtilities.getOptionValueAsInt("gradient_ascent_training_iterations");
        }

        learningRate = 0.01;
        if (CommandLineUtilities.hasArg("gradient_ascent_learning_rate")) {
            learningRate = CommandLineUtilities.getOptionValueAsFloat("gradient_ascent_learning_rate");
        }
    }

    protected abstract double kernelFunction(FeatureVector fv1, FeatureVector fv2);

    private void cacheAlphaDotGramMatrix(double[] alphaVector, double[][] gramMatrix) {
        for (int i = 0; i < gramMatrix.length; i++) {
            double summation = 0;
            for (int j = 0; j < gramMatrix.length; j++) {
                summation += alphaVector[j] * gramMatrix[j][i];
            }
            preCachedAlphaDotGramMatrix[i] = summation;
        }
    }

    private void initializeGramMatrixAndAlphaVector(int noOfInstances) {
        alphaVector = new double[noOfInstances];
        gramMatrix = new double[noOfInstances][noOfInstances];
        preCachedAlphaDotGramMatrix = new double[noOfInstances];
    }

    private double linkFunction(double input) {
        return 1.0 / (1.0 + Math.exp(-1.0 * input));
    }

    @Override
    public void train(List<Instance> instances) {
        trainingInstances = instances;

        initializeGramMatrixAndAlphaVector(trainingInstances.size());
        computeGramMatrix(trainingInstances, gramMatrix, trainingInstances.size());

        while (iterations-- > 0) {
            cacheAlphaDotGramMatrix(alphaVector, gramMatrix);
            for (int k = 0; k < alphaVector.length; k++) {
                double gradient = computeGradient(trainingInstances, k);
                updateAlphaVector(k, alphaVector, learningRate, gradient);
            }
        }
    }

    @Override
    public Label predict(Instance instance) {
        double linkFunctionOutput = 0;
        double summation = 0;
        for (int j = 0; j < trainingInstances.size(); ++j) {
            summation += alphaVector[j] * kernelFunction(trainingInstances.get(j).getFeatureVector(),
                    instance.getFeatureVector());
            linkFunctionOutput = linkFunction(summation);
        }
        return linkFunctionOutput >= 0.5 ? new ClassificationLabel(1) : new ClassificationLabel(0);
    }

    protected void computeGramMatrix(List<Instance> instances, double[][] gramMatrix, int noOfInstances) {
        for (int i = 0; i < noOfInstances; ++i) {
            for (int j = 0; j < noOfInstances; ++j) {
                gramMatrix[i][j] = kernelFunction(instances.get(i).getFeatureVector(),
                        instances.get(j).getFeatureVector());
            }
        }
    }

    protected double computeGradient(List<Instance> instances, int k) {
        double gradient = 0;
        for (int i = 0; i < instances.size(); i++) {
            double labelValue = instances.get(i).getLabel().getLabelValue();
            if (labelValue == 1) {
                double linkFunctionOutput = linkFunction(-1 * preCachedAlphaDotGramMatrix[i]);
                gradient += linkFunctionOutput * gramMatrix[i][k];
            } else if (labelValue == 0) {
                double linkFunctionOutput = linkFunction(preCachedAlphaDotGramMatrix[i]);
                gradient += linkFunctionOutput * (-1 * gramMatrix[i][k]);
            }
        }
        return gradient;
    }

    protected void updateAlphaVector(int k, double[] alphaVector, double learningRate, double gradient) {
        alphaVector[k] += learningRate * gradient;
    }

    protected double computeLinearCombination(FeatureVector fv1, FeatureVector fv2) {
        double dotProduct = 0.0;

        for (Integer feature : fv1.getFeatureVectorKeys()) {
            if (fv2.getFeatureVectorKeys().contains(feature)) {
                dotProduct += fv1.get(feature) * fv2.get(feature);
            }
        }

        return dotProduct;
    }

}
