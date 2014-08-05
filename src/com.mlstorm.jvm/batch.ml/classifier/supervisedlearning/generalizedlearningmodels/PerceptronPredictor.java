package classifier.supervisedlearning.generalizedlearningmodels;

import dataobject.FeatureVector;
import dataobject.Instance;
import dataobject.label.Label;

import java.util.HashMap;
import java.util.List;

public class PerceptronPredictor extends LinearThresholdClassifierBase {

    @Override
    public void initializeParametersToDefaults() {
        thickness = 0;
        setLearningRateEeta(1);
        scalarThresholdBeta = 0;
    }

    protected void initializeWeights(List<Instance> instances) {
        int n = getTotalNoOfFeatures(instances);
        for (int i = 1; i <= n; i++) {
            getWeightVectorW().put(i, 0.0);
        }
    }


    @Override
    protected void updateWeight(
            Label yi, FeatureVector fv, HashMap<Integer, Double> weightVectorW,
            double learningRate) {
        double yiValue = yi.getLabelValue();

        for (Integer feature : fv.getFeatureVectorKeys()) {
            double oldWeight = weightVectorW.get(feature);
            double newWeight = oldWeight + learningRate * yiValue * fv.get(feature);
            weightVectorW.put(feature, newWeight);
        }
    }

}
