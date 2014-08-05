package classifier.supervisedlearning.decisiontreetrainer;

import dataobject.Instance;
import dataobject.label.Label;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class C45DecisionTreeTrainer {

    private HashMap<Integer, Double> conditionalEntropy;  // H(Y|X)
    private HashMap<Integer, HashMap<Integer, Integer>> countOfBinaryFeatureFiring;
    private HashMap<Label/*outputLabel*/,
            HashMap<Integer /*columnIndex*/,
                    HashMap<Double /*Value*/, Double /*frequency*/>>> labelToValueFrequencyMultiMap;


    public C45DecisionTreeTrainer() {
        conditionalEntropy = new HashMap<Integer, Double>();
        labelToValueFrequencyMultiMap = new HashMap<Label, HashMap<Integer, HashMap<Double, Double>>>();
        countOfBinaryFeatureFiring = new HashMap<Integer, HashMap<Integer, Integer>>();
    }

    private void cookFrequencyOfFeatureValues(List<Instance> instances, HashMap<Integer, Boolean> featureTypes,
                                              HashMap<Integer, Double> precomputedMeans) {
        int totalFeatures = featureTypes.size();

        for (Instance instance : instances) {
            for (int feature = 1; feature <= totalFeatures; ++feature) {
                double value = 0;
                double mean = precomputedMeans.get(feature);
                if (instance.getFeatureVector().getFeatureVectorKeys().contains(feature)) {
                    value = instance.getFeatureVector().get(feature);
                }

                HashMap<Integer, HashMap<Double, Double>> featureValueCountMap;

                if (labelToValueFrequencyMultiMap.containsKey(instance.getLabel())) {
                    featureValueCountMap = labelToValueFrequencyMultiMap.get(instance.getLabel());
                } else {
                    featureValueCountMap = new HashMap<Integer, HashMap<Double, Double>>();
                    labelToValueFrequencyMultiMap.put(instance.getLabel(), featureValueCountMap);
                }

                int binaryFeature; // this is the value that we are counting
                if ((featureTypes.get(feature) && value == 1) || value > mean) {
                    value = 1;
                    binaryFeature = 1;
                    incrementBinaryInterpretationOfFeature(feature, binaryFeature);
                } else {
                    value = 0;
                    binaryFeature = 0;
                    incrementBinaryInterpretationOfFeature(feature, binaryFeature);
                }

                HashMap<Double, Double> valueCountMap = incrementFeatureValueCount(feature, value,
                        featureValueCountMap);
                featureValueCountMap.put(feature, valueCountMap);
            }
        }
    }

    private Integer getFeatureWithBestIG(List<Instance> instances) {
        for (Label yi : labelToValueFrequencyMultiMap.keySet()) {
            HashMap<Integer, HashMap<Double, Double>> featuresToValueFrequencyMap = labelToValueFrequencyMultiMap.get
                    (yi);
            for (Integer featureXj /* A particular feature */ : featuresToValueFrequencyMap.keySet()) {
                int countXjHigh = 0, countXjLow = 0, high = 1, low = 0;
                double countXjYiHigh = 0, countXjYiLow = 0, entropyOfYiGivenXi = 0.0;

                if (countOfBinaryFeatureFiring.get(featureXj).containsKey(high)) {
                    countXjHigh = countOfBinaryFeatureFiring.get(featureXj).get(high);
                }

                if (countOfBinaryFeatureFiring.get(featureXj).containsKey(low)) {
                    countXjLow = countOfBinaryFeatureFiring.get(featureXj).get(low);
                }

                HashMap<Double, Double> valueXjCount = featuresToValueFrequencyMap.get(featureXj);
                for (Map.Entry<Double, Double> valueXj : valueXjCount.entrySet()) {
                    if (valueXj.getKey() == high) {
                        countXjYiHigh += valueXj.getValue();
                    } else {
                        countXjYiLow += valueXj.getValue();
                    }
                }

                double pOfYiXjLow = countXjYiLow / instances.size();
                double pOfYiXjHigh = countXjYiHigh / instances.size();

                if (countXjYiLow > 0) {
                    entropyOfYiGivenXi -= pOfYiXjLow * Math.log(countXjYiLow / countXjLow) / Math.log(2);
                }
                if (countXjYiHigh > 0) {
                    entropyOfYiGivenXi -= pOfYiXjHigh * Math.log(countXjYiHigh / countXjHigh) / Math.log(2);
                }

                if (conditionalEntropy.containsKey(featureXj)) {
                    entropyOfYiGivenXi = conditionalEntropy.get(featureXj) + entropyOfYiGivenXi;
                }

                conditionalEntropy.put(featureXj, entropyOfYiGivenXi);
            }
        }
        printEntropyStatistics();
        return returnFeatureWithMinEntropy(conditionalEntropy);
    }

    private void incrementBinaryInterpretationOfFeature(int feature, int binaryFeature) {
        if (countOfBinaryFeatureFiring.containsKey(feature)) {
            if (countOfBinaryFeatureFiring.get(feature).containsKey(binaryFeature)) {
                int cnt = countOfBinaryFeatureFiring.get(feature).get(binaryFeature);
                cnt++;
                countOfBinaryFeatureFiring.get(feature).put(binaryFeature, cnt);
            } else {
                countOfBinaryFeatureFiring.get(feature).put(binaryFeature, 1);
            }
        } else {
            HashMap<Integer, Integer> binaryFeaturesWithCount = new HashMap<Integer, Integer>();
            binaryFeaturesWithCount.put(binaryFeature, 1);
            countOfBinaryFeatureFiring.put(feature, binaryFeaturesWithCount);
        }
    }

    private HashMap<Double, Double> incrementFeatureValueCount(
            int feature,
            double valueOfThisFeature,
            HashMap<Integer, HashMap<Double, Double>> featureValueCountMap
    ) {
        HashMap<Double, Double> valueCountMap;
        if (featureValueCountMap.containsKey(feature)) {
            valueCountMap = featureValueCountMap.get(feature);
            double countOfValue = 0;

            if (valueCountMap.containsKey(valueOfThisFeature)) {
                countOfValue = valueCountMap.get(valueOfThisFeature);
            }
            // Increment frequency and add it back to the map
            countOfValue++;
            valueCountMap.put(valueOfThisFeature, countOfValue);
        } else {
            valueCountMap = new HashMap<Double, Double>();
            valueCountMap.put(valueOfThisFeature, 1.0 /*initial frequency*/);
        }
        return valueCountMap;
    }

    private void printEntropyStatistics() {
        Double maxEntropy = 0.0, minEntropy = 0.0;
        int noOfZeros = 0;
        for (Double value : conditionalEntropy.values()) {
            if (value > maxEntropy) {
                maxEntropy = value;
            }
            if (value == 0.0) {
                noOfZeros++;
            }
            if (value < minEntropy) {
                minEntropy = value;
            }
        }
    }

    private Integer returnFeatureWithMinEntropy(HashMap<Integer, Double> entropyOfFeatures) {
        double minEntropy = Double.MAX_VALUE;
        int featureWithMinEntropy = entropyOfFeatures.isEmpty() ? -1 : entropyOfFeatures.keySet().iterator().next();

        for (Map.Entry<Integer, Double> entry : entropyOfFeatures.entrySet()) {
            if (entry.getValue() < minEntropy) {
                minEntropy = entry.getValue();
            }
        }

        for (Map.Entry<Integer, Double> entry : entropyOfFeatures.entrySet()) {
            if (entry.getValue() == minEntropy) {
                return entry.getKey();
            }
        }
        return featureWithMinEntropy;
    }

    public int getFeatureWithLeastEntropy(List<Instance> instances, HashMap<Integer, Boolean> featureTypes,
                                          HashMap<Integer, Double> precomputedMeans) {
        cookFrequencyOfFeatureValues(instances, featureTypes, precomputedMeans);
        return getFeatureWithBestIG(instances);
    }
}
