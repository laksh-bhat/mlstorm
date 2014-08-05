package classifier.unsupervisedlearning;

import classifier.Predictor;
import dataobject.FeatureVector;
import dataobject.Instance;
import dataobject.label.ClassificationLabel;
import dataobject.label.Label;
import utils.CommandLineUtilities;
import utils.UtilityFunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LambdaMeansPredictor extends Predictor {
    private int numberOfFeatures;
    private int trainingIterations;

    /* private double computeEuclidianDistance ( FeatureVector fv1, FeatureVector fv2 )
     {
         double distance = 0;
         for ( int i = 1 ; i <= numberOfFeatures ; ++i )
         {
             double fv1Value = 0, fv2Value = 0, difference;
             if ( fv1.getFeatureVectorKeys().contains( i ) )
             {
                 fv1Value = fv1.get( i );
             }

             if ( fv2.getFeatureVectorKeys().contains( i ) )
             {
                 fv2Value = fv2.get( i );
             }

             difference = fv1Value - fv2Value;
             distance += Math.pow( difference, 2 );
         }

         return Math.sqrt( distance );
     }*/
    private double thresholdLambda;
    private HashMap<Integer, FeatureVector> prototypes;
    private HashMap<Integer, HashMap<Integer, FeatureVector>> clusterAssignments;

    public LambdaMeansPredictor() {
        getClusterLambda();
        getLambdaClusterTrainingIterations();
        setPrototypeVector(new HashMap<Integer, FeatureVector>());
        clusterAssignments = new HashMap<Integer, HashMap<Integer, FeatureVector>>();
    }

    private void addNewClusterPrototype(
            int clusterName, HashMap<Integer, FeatureVector> prototypeVector, FeatureVector prototype) {
        prototypeVector.put(clusterName, prototype);
        //setClustersCount( getClustersCount() + 1 );
    }

    private void assignInstancesToClusters(List<Instance> instances) {
        for (int i = 0; i < instances.size(); ++i) {
            int cluster = 0;
            double minDistance = Double.MAX_VALUE;

            for (int clusterK : getPrototypeVector().keySet()) {
                double distance = UtilityFunctions.computeL2Norm(instances.get(i).getFeatureVector(),
                        getPrototypeVector().get(clusterK));
                if (distance < minDistance) {
                    minDistance = distance;
                    cluster = clusterK;
                }
            }

            handleClusterAssignment(instances, i, cluster, minDistance);
        }
    }

    private void cleanup() {
        clusterAssignments.clear();
    }

    private FeatureVector computePrototype(List<Instance> instances) {
        FeatureVector meanVector = new FeatureVector();
        for (Instance instance : instances) {
            FeatureVector fv = instance.getFeatureVector();
            for (int feature : fv.getFeatureVectorKeys()) {
                if (meanVector.getFeatureVectorKeys().contains(feature)) {
                    meanVector.add(feature, meanVector.get(feature) + fv.get(feature));
                } else {
                    meanVector.add(feature, fv.get(feature));
                }
            }
        }

        for (int feature : meanVector.getFeatureVectorKeys()) {
            meanVector.add(feature, meanVector.get(feature) / instances.size());
        }

        return meanVector;
    }

    private HashMap<Integer, FeatureVector> getClusterAssignment(int index) {
        HashMap<Integer, FeatureVector> row;
        if (!clusterAssignments.containsKey(index)) {
            row = new HashMap<Integer, FeatureVector>();
        } else {
            row = clusterAssignments.get(index);
        }
        return row;
    }

    private double getClusterLambda() {
        if (thresholdLambda != 0) {
            return thresholdLambda;
        }

        if (CommandLineUtilities.hasArg("cluster_lambda")) {
            thresholdLambda = CommandLineUtilities.getOptionValueAsFloat("cluster_lambda");
        }

        return thresholdLambda;
    }

    private void getLambdaClusterTrainingIterations() {
        int clustering_training_iterations = 10;
        if (CommandLineUtilities.hasArg("clustering_training_iterations")) {
            clustering_training_iterations = CommandLineUtilities.getOptionValueAsInt(
                    "clustering_training_iterations");
        }

        trainingIterations = clustering_training_iterations;
    }

    private void handleClusterAssignment(
            List<Instance> instances,
            int instanceIndex,
            int cluster,
            double minDistance) {
        if (minDistance <= getClusterLambda()) {
            HashMap<Integer, FeatureVector> clusterAssignmentForInstance = getClusterAssignment(instanceIndex);
            clusterAssignmentForInstance.clear();
            clusterAssignmentForInstance.put(cluster, getPrototypeVector().get(cluster));
            clusterAssignments.put(instanceIndex, clusterAssignmentForInstance);
        } else {
            // new cluster
            cluster = getClustersCount();
            // the only vector in the cluster
            FeatureVector prototype = instances.get(instanceIndex).getFeatureVector();

            addNewClusterPrototype(cluster, getPrototypeVector(), prototype);

            HashMap<Integer, FeatureVector> clusterAssignmentForInstance = getClusterAssignment(instanceIndex);
            clusterAssignmentForInstance.clear();   // remember any instance belongs to
            // only one cluster at any point.
            clusterAssignmentForInstance.put(cluster, prototype);
            clusterAssignments.put(instanceIndex, clusterAssignmentForInstance);
        }
    }

    private void initializePrototypeAndSetThreshold(List<Instance> instances) {
        FeatureVector meanVector = computePrototype(instances);
        if (thresholdLambda == 0) {
            double distancesFromMean = 0;
            for (Instance instance : instances) {
                distancesFromMean += UtilityFunctions.computeL2Norm(instance.getFeatureVector(), meanVector);
            }
            thresholdLambda = distancesFromMean / instances.size();
        }
        addNewClusterPrototype(getClustersCount(), prototypes, meanVector);
    }

    private void updateMeanVectors(HashMap<Integer, FeatureVector> prototypes, List<Instance> instances) {
        int noOfClusters = prototypes.size();
        // TODO : This should clear-up memory
        prototypes.clear();

        for (int cluster = 0; cluster < noOfClusters; cluster++) {
            List<Instance> clusterInstances = new ArrayList<Instance>();

            for (int i = 0; i < instances.size(); ++i) {
                HashMap<Integer, FeatureVector> row = clusterAssignments.get(i);
                if (row.containsKey(cluster)) {
                    clusterInstances.add(instances.get(i));
                }
            }
            prototypes.put(cluster, computePrototype(clusterInstances));
        }
    }

    @Override
    public void train(List<Instance> instances) {
        numberOfFeatures = UtilityFunctions.getNumberOfFeatures(instances);
        initializePrototypeAndSetThreshold(instances);

        for (int i = 0; i < trainingIterations; ++i) {
            // E-Step
            // TODO : clear some memory
            clusterAssignments.clear();
            System.gc();
            assignInstancesToClusters(instances);

            // M-Step
            updateMeanVectors(getPrototypeVector(), instances);
        }
        cleanup();
    }

    @Override
    public Label predict(Instance instance) {
        int cluster = 0;
        double minDistance = Double.MAX_VALUE;

        for (int clusterK : getPrototypeVector().keySet()) {
            double distance = UtilityFunctions.computeL2Norm(instance.getFeatureVector(),
                    getPrototypeVector().get(clusterK));

            if (distance < minDistance) {
                minDistance = distance;
                cluster = clusterK;
            }
        }
        return new ClassificationLabel(cluster);
    }

    public int getClustersCount() {
        return prototypes.size();
    }

    public HashMap<Integer, FeatureVector> getPrototypeVector() {
        return prototypes;
    }

    public void setPrototypeVector(HashMap<Integer, FeatureVector> prototypeVector) {
        this.prototypes = prototypeVector;
    }
}
