package classifier.supervisedlearning.simpleclassifier;

import classifier.Predictor;
import dataobject.Instance;
import dataobject.label.Label;

import java.util.*;

public class MajorityClassifier extends Predictor {
    private Label maxLabel;
    private Set<Label> maxLabels;
    private HashMap<Label, Long> labelFrequency;

    public MajorityClassifier() {
        maxLabels = new HashSet<Label>();
        labelFrequency = new HashMap<Label, Long>();
    }

    private void chooseMaxLabelFromSet() {
        Random rand = new Random();
        Object[] labels = maxLabels.toArray();
        int min = 0, max = labels.length;
        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt(max - min) + min;
        setMaxLabel((Label) labels[randomNum]);
    }


    // This works for a sorted map implementation which I think is unnecessary here.

    /* private void generateMaxLabelSet() {
        Long maxVal;
        do {
            maxVal = labelFrequency.get(labelFrequency.lastKey());
            maxLabels.add(labelFrequency.lastKey());
            labelFrequency.remove(labelFrequency.lastKey());
        } while (maxVal >= labelFrequency.get(labelFrequency.lastKey()));
    }*/

    private void cleanUp() {
        labelFrequency.clear();
        maxLabels.clear();
    }

    private void computeLabelFrequency(List<Instance> instances) {
        for (Instance instance : instances) {
            Label label = instance.getLabel();
            Long value = labelFrequency.get(label);
            if (value == null) {
                labelFrequency.put(label, (long) 1);
            } else {
                labelFrequency.remove(label);
                labelFrequency.put(label, ++value);
            }
        }
    }

    // This implementation is simple but non-performing perhaps
    private void generateMaxLabelSetEx2() {
        Long maxVal = Long.MIN_VALUE;
        for (Long val : labelFrequency.values()) {
            if (val > maxVal) {
                maxVal = val;
            }
        }

        maxLabels = getKeysBasedOnValue(labelFrequency, maxVal);
    }

    @Override
    public void train(List<Instance> instances) {
        computeLabelFrequency(instances);
        generateMaxLabelSetEx2();
        chooseMaxLabelFromSet();
        cleanUp();
    }

    @Override
    public Label predict(Instance instance) {
        return getMaxLabel();
    }

    public Label getMaxLabel() {
        return maxLabel;
    }

    public void setMaxLabel(Label maxLabel) {
        this.maxLabel = maxLabel;
    }
}
