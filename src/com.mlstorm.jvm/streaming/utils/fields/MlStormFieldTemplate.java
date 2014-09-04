package utils.fields;

import java.text.MessageFormat;

/**
 * This is the default feature identification template used by MlStorm.
 * If you want to use different field names just implement the FeatureTemplate and provide key and feature vector fields
 * to the updaters and the query modules.
 */
public final class MlStormFieldTemplate implements FieldTemplate {
    /**
     * These fields are used to emit, update and query the model.
     */
    private final String[] fields;
    private int numFeatures;

    public MlStormFieldTemplate() {
        fields = new String[]{"key", "featureVector"};
    }

    public MlStormFieldTemplate(String[] fields) {
        if (fields.length < 2) {
            throw new IllegalStateException("Programmer error: Fields must have at least one key and a feature vector");
        }
        this.fields = fields;
    }

    @Override
    public String[] getFields() {
        return fields;
    }

    /**
     * A default implementation
     *
     * @return
     */
    @Override
    public String getKeyField() {
        return fields[0];
    }

    /**
     * A default implementation
     *
     * @return
     */
    @Override
    public String getFeatureVectorField() {
        return fields[1];
    }

    @Override
    public int getRuntimeFeatureCount() {
        if (numFeatures == 0) {
            throw new IllegalStateException(MessageFormat.format("Programmer error: {0}.numFeatures has not been updated by the MlStormSpout", getClass().getCanonicalName()));
        }
        return numFeatures;
    }

    @Override
    public void setRuntimeFeatureCount(int numFeatures) {
        this.numFeatures = numFeatures;
    }
}
