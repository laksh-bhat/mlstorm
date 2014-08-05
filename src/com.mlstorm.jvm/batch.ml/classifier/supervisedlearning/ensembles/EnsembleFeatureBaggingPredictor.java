package classifier.supervisedlearning.ensembles;

import classifier.supervisedlearning.generalizedlearningmodels.PerceptronPredictor;
import dataobject.Instance;
import dataobject.label.Label;

import java.util.ArrayList;
import java.util.List;


public class EnsembleFeatureBaggingPredictor extends EnsemblePredictor {
    public void train(List<Instance> instances) {
        int noOfClassifiers = getNoOfClassifiers();
        trainClassifiers(instances, noOfClassifiers);
        int trainingIterations = getTrainingIterations();

        while (trainingIterations-- > 0) {
            for (Instance instance : instances) {
                Label yi = instance.getLabel();
                Label yCap = predictLabel(noOfClassifiers, instance);

                if (yCap.getLabelValue() != yi.getLabelValue()) {
                    updateWeightOfClassifier(noOfClassifiers, instance);
                }
            }
        }
    }

    @Override
    // implements feature bagging
    protected List<Instance> getInstancesForEnsembleTraining(int classifierIndex, int K, List<Instance> instances) {
        int totalFeatures = PerceptronPredictor.getTotalNoOfFeatures(instances);
        List<Instance> featureBag = new ArrayList<Instance>();
        for (Instance instance : instances) {
            try {
                Instance clone = (Instance) instance.clone();
                for (int feature = 1; feature <= totalFeatures; feature++) {
                    if ((feature % K != classifierIndex)) {
                        if (clone.getFeatureVector().getFeatureVectorKeys().contains(feature)) {
                            clone.getFeatureVector().featureVector.remove(feature);
                        }
                    }
                }
                featureBag.add(clone);
            } catch (CloneNotSupportedException ignored) {
            }
        }
        return featureBag;
    }
}
