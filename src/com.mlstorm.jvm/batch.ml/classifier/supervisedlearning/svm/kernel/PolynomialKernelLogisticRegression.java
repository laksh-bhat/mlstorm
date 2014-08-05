package classifier.supervisedlearning.svm.kernel;

import dataobject.FeatureVector;
import utils.CommandLineUtilities;

public class PolynomialKernelLogisticRegression extends KernelLogisticRegression {

    @Override
    protected double kernelFunction(FeatureVector fv1, FeatureVector fv2) {
        double polynomialKernelExponent = 2;
        if (CommandLineUtilities.hasArg("polynomial_kernel_exponent")) {
            polynomialKernelExponent = CommandLineUtilities.getOptionValueAsFloat("polynomial_kernel_exponent");
        }

        return Math.pow(1 + computeLinearCombination(fv1, fv2), polynomialKernelExponent);
    }
}
