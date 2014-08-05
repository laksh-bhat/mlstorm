package dataobject.label;

public class RegressionLabel implements Label {

    private double regressionLabel;

    public RegressionLabel(double label) {
        regressionLabel = label;
    }

    @Override
    public String toString() {
        return String.valueOf(regressionLabel);
    }

    @Override
    public double getLabelValue() {
        return regressionLabel;
    }
}
