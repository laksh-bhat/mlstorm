package classifier.supervisedlearning.svm.kernel;

import dataobject.FeatureVector;
import utils.CommandLineUtilities;
import utils.UtilityFunctions;


public class GaussianKernelLogisticRegression extends KernelLogisticRegression {

    private double gaussianKernelSigma;

    public GaussianKernelLogisticRegression() {
        gaussianKernelSigma = 1;
        if (CommandLineUtilities.hasArg("gaussian_kernel_sigma")) {
            gaussianKernelSigma = CommandLineUtilities.getOptionValueAsFloat("gaussian_kernel_sigma");
        }
    }

    protected double kernelFunction(FeatureVector fv1, FeatureVector fv2) {
        return Math.exp(-1 * UtilityFunctions.computeL2Norm(fv1, fv2) / 2 * gaussianKernelSigma);
    }
}
