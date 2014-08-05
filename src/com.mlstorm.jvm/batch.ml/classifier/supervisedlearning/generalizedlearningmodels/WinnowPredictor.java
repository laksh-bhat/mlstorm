package classifier.supervisedlearning.generalizedlearningmodels;

import dataobject.FeatureVector;
import dataobject.Instance;
import dataobject.label.Label;

import java.util.HashMap;
import java.util.List;

public class WinnowPredictor extends classifier.supervisedlearning.generalizedlearningmodels.LinearThresholdClassifierBase {
    private double mu = Math.pow(10, 6);

    private int sign(double v) {
        if (v > 0) {
            return 1;
        } else if (v < 0) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public void initializeParametersToDefaults() {
        setLearningRateEeta(2.0);
    }

    protected void initializeWeights(List<Instance> instances) {
        int n = getTotalNoOfFeatures(instances);
        for (int i = 1; i <= n; i++) {
            getWeightVectorW().put(i, 1.0);
        }
        scalarThresholdBeta = instances.size() / 2;
    }

    @Override
    protected void updateWeight(Label yi, FeatureVector fv, HashMap<Integer, Double> weightVectorW,
                                double learningRate) {
        for (int feature : fv.getFeatureVectorKeys()) {
            double yiValue = yi.getLabelValue();
            Double wDash = weightVectorW.get(feature) * Math.pow(learningRate, yiValue * sign(fv.get(feature)));
            weightVectorW.put(feature, (wDash > mu) ? mu : wDash);
        }
    }
}
