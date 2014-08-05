package classifier.supervisedlearning.simpleclassifier;

import classifier.Predictor;
import dataobject.Instance;
import dataobject.label.ClassificationLabel;
import dataobject.label.Label;
import dataobject.label.RegressionLabel;

import java.text.MessageFormat;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EvenOddClassifier extends Predictor {
    Label predictedLabel;
    private double oddSum;
    private double evenSum;

    private void computeEvenOddSums(Instance instance) {
        for (int key : instance.getFeatureVector().getFeatureVectorKeys()) {
            if (key % 2 == 0) {
                evenSum += instance.getFeatureVector().get(key);
            } else {
                oddSum += instance.getFeatureVector().get(key);
            }
        }
    }

    private void createNewClassificationLabel() {
        Logger.getAnonymousLogger().log(Level.INFO, "Creating new ClassificationLabel based on input instance.");
        if (evenSum >= oddSum) {
            predictedLabel = new ClassificationLabel(1);
        } else {
            predictedLabel = new ClassificationLabel(0);
        }
    }

    private void createNewRegressionLabel() {
        Logger.getAnonymousLogger().log(Level.INFO, "Creating new RegressionLabel based on input instance.");
        if (evenSum >= oddSum) {
            predictedLabel = new RegressionLabel(1);
        } else {
            predictedLabel = new RegressionLabel(0);
        }
    }

    @Override
    public void train(List<Instance> instances) {
        // no-op
    }

    @Override
    public Label predict(Instance instance) {

        evenSum = 0;
        oddSum = 0;
        computeEvenOddSums(instance);
        Logger.getAnonymousLogger().log(Level.INFO, MessageFormat.format("Training EvenOddClassifier complete. evenSum = {0}, oddSum = {1}", evenSum, oddSum));

        if (instance.getLabel() instanceof RegressionLabel) {
            createNewRegressionLabel();
        } else {
            createNewClassificationLabel();
        }
        Logger.getAnonymousLogger().log(Level.INFO, MessageFormat.format("Predicted label {0}", predictedLabel.getLabelValue()));
        return predictedLabel;
    }
}
