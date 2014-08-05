package dataobject.label;

public class ClassificationLabel implements Label {

    private int classificationLabel;

    public ClassificationLabel(int label) {
        classificationLabel = label;
    }

    @Override
    public String toString() {
        return String.valueOf(classificationLabel);
    }

    @Override
    public double getLabelValue() {
        return classificationLabel;
    }

    public void setLabelValue(int value) {
        classificationLabel = value;
    }

    // According to Joshua Block's Effective Java
    public int hashCode() {
        return 197 * 17 + this.toString().hashCode();
    }

    public boolean equals(Object inputObject) {
        if (inputObject == null) {
            return false;
        }
        if (inputObject == this) {
            return true;
        }
        if (inputObject.getClass() != getClass()) {
            return false;
        }

        final Label label = (Label) inputObject;
        return this.toString().equalsIgnoreCase(label.toString());
    }
}
