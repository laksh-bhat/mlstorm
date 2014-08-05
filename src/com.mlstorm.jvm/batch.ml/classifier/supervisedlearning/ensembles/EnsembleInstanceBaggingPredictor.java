package classifier.supervisedlearning.ensembles;

import dataobject.Instance;
import dataobject.label.Label;

import java.util.ArrayList;
import java.util.List;

public class EnsembleInstanceBaggingPredictor extends EnsemblePredictor {
    @Override
    public void train(List<Instance> instances) {
        int noOfClassifiers = getNoOfClassifiers();
        trainClassifiers(instances, noOfClassifiers);
        int trainingIterations = getTrainingIterations();

        while (trainingIterations > 0) {
            for (Instance instance : instances) {
                Label yi = instance.getLabel();
                Label yCap = predictLabel(noOfClassifiers, instance);

                if (yCap.getLabelValue() != yi.getLabelValue()) {
                    updateWeightOfClassifier(noOfClassifiers, instance);
                }
            }

            trainingIterations -= 1;
        }
    }


    @Override
    // Implements Instance bagging
    protected List<Instance> getInstancesForEnsembleTraining(int classifier, int K, List<Instance> instances) {
        List<Instance> instanceBag = new ArrayList<Instance>();
        for (int i = 0; i < instances.size(); i++) {
            if (!(i % K == classifier)) {
                instanceBag.add(instances.get(i));
            }
        }
        return instanceBag;
    }
}
