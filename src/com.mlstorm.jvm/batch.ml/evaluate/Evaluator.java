package evaluate;

import classifier.Predictor;
import dataobject.Instance;

import java.util.List;

public abstract class Evaluator {

    public abstract double evaluate(List<Instance> instances, Predictor predictor);
}
