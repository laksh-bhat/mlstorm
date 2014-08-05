package classifier.supervisedlearning.svm.kernel;

import dataobject.FeatureVector;

public class LinearKernelLogisticRegression extends KernelLogisticRegression {
    protected double kernelFunction(FeatureVector fv1, FeatureVector fv2) {
        return computeLinearCombination(fv1, fv2);
    }
}
