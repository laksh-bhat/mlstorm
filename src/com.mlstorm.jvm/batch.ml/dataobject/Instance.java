package dataobject;

import dataobject.label.Label;

import java.io.Serializable;

public class Instance implements Serializable, Cloneable {

    Label _label = null;
    FeatureVector _feature_vector = null;

    public Instance(FeatureVector feature_vector, Label label) {
        this._feature_vector = feature_vector;
        this._label = label;
    }

    public Label getLabel() {
        return _label;
    }

    public void setLabel(Label label) {
        this._label = label;
    }

    public FeatureVector getFeatureVector() {
        return _feature_vector;
    }

    public void setFeatureVector(FeatureVector feature_vector) {
        this._feature_vector = feature_vector;
    }

    public Object clone() throws CloneNotSupportedException {
        Instance cloned = new Instance((FeatureVector) this._feature_vector.clone(), this.getLabel());
        super.clone();
        return cloned;
    }
}
