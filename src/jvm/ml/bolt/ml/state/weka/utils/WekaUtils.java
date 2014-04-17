package bolt.ml.state.weka.utils;

import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayesUpdateable;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.classifiers.functions.SGD;
import weka.classifiers.functions.SMO;
import weka.classifiers.functions.SimpleLogistic;
import weka.classifiers.lazy.IBk;
import weka.classifiers.lazy.LWL;
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.HoeffdingTree;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.RandomForest;
import weka.clusterers.*;
import weka.core.Attribute;
import weka.filters.AllFilter;

import java.text.MessageFormat;
import java.util.ArrayList;

public class WekaUtils {

    public static ArrayList<Attribute> makeFeatureVectorForOnlineClustering(int noOfClusters, int noOfAttributes) {
        // Declare FAST VECTOR
        ArrayList<Attribute> attributeInfo = new ArrayList<Attribute>();

        // Declare FEATURES and add them to FEATURE VECTOR
        for (int i = 0; i < noOfAttributes; i++)
            attributeInfo.add(new Attribute(MessageFormat.format("feature-{0}", i)));

        System.err.println("DEBUG: no. of attributes = " + attributeInfo.size());
        return attributeInfo;
    }

    public static ArrayList<Attribute> makeFeatureVectorForBatchClustering(int noOfClusters, int noOfAttributes) {
        // Declare FAST VECTOR
        ArrayList<Attribute> attributeInfo = new ArrayList<Attribute>();

        // Declare FEATURES and add them to FEATURE VECTOR
        for (int i = 0; i < noOfAttributes; i++)
            attributeInfo.add(new Attribute(MessageFormat.format("feature-{0}", i)));

        ArrayList<String> clusters = new ArrayList<String>(noOfClusters);
        for (int i = 1; i <= noOfClusters; i++)
            clusters.add(MessageFormat.format("class-{0}", String.valueOf(i)));
        Attribute cluster = new Attribute("classes", clusters);
        // last element in a FEATURE VECTOR is the category
        attributeInfo.add(cluster);

        System.err.println("DEBUG: no. of attributes = " + attributeInfo.size());
        return attributeInfo;
    }

    public static ArrayList<Attribute> makeFeatureVectorForBinaryClassification(int noOfAttributes) {
        ArrayList<Attribute> attributeInfo = new ArrayList<Attribute>();
        // Declare FEATURES and add them to FEATURE VECTOR
        for (int i = 0; i < noOfAttributes; i++)
            attributeInfo.add(new Attribute(MessageFormat.format("feature-{0}", i)));
        // last element in a FEATURE VECTOR is the category
        ArrayList<String> classNames = new ArrayList<String>(2);
        for (int i = 1; i <= 2; i++)
            classNames.add(MessageFormat.format("class-{0}", String.valueOf(i)));
        Attribute classes = new Attribute("classes", classNames);
        // last element in a FEATURE VECTOR is the category
        attributeInfo.add(classes);
        return attributeInfo;
    }

    public static Classifier makeClassifier(String wekaClassifier) {
        switch (WekaClassificationAlgorithms.valueOf(wekaClassifier)) {
            case decisionTree:
                return new J48();
            case svm:
                return new SMO();
            case logisticRegression:
                return new SimpleLogistic();
            case randomForest:
                return new RandomForest();
            case decisionStump:
                return new DecisionStump();
            case perceptron:
                return new MultilayerPerceptron();
            default:
                return new SMO();
        }
    }

    public static Classifier makeOnlineClassifier(String wekaClassifier) {
        switch (WekaOnlineClassificationAlgorithms.valueOf(wekaClassifier)) {
            case naiveBayes:
                return new NaiveBayesUpdateable();
            case locallyWeightedLearner:
                return new LWL();
            case nearestNeighbors:
                return new IBk();
            case onlineDecisionTree:
                return new HoeffdingTree();
            case stochasticGradientDescent:
                return new SGD();
            default:
                return new NaiveBayesUpdateable();
        }
    }

    public static Clusterer makeClusterer(String wekaClassifier, int numClusters) throws Exception {
        try {
            switch (WekaClusterers.valueOf(wekaClassifier)) {
                case kmeans:
                    SimpleKMeans kmeans = new SimpleKMeans();
                    kmeans.setNumClusters(numClusters);
                    return kmeans;
                case densityBased:
                    MakeDensityBasedClusterer clusterer = new MakeDensityBasedClusterer();
                    clusterer.setNumClusters(numClusters);
                    return clusterer;
                case farthestFirst:
                    FarthestFirst ff = new FarthestFirst();
                    ff.setNumClusters(numClusters);
                    return ff;
                case hierarchicalClusterer:
                    HierarchicalClusterer hc = new HierarchicalClusterer();
                    hc.setNumClusters(numClusters);
                    return hc;
                case em:
                    EM em = new EM();
                    em.setMaxIterations(10);
                    em.setMaximumNumberOfClusters(numClusters);
                    em.setNumClusters(numClusters);
                    return em;
                case filteredClusterer:
                    kmeans = new SimpleKMeans();
                    kmeans.setNumClusters(numClusters);
                    FilteredClusterer fc = new FilteredClusterer();
                    fc.setFilter(new AllFilter());
                    fc.setClusterer(kmeans);
                    return fc;
                default:
                    kmeans = new SimpleKMeans();
                    kmeans.setNumClusters(numClusters);
                    return kmeans;
            }
        } catch (Exception e) {
            throw new Exception("Could not make Clusterer", e);
        }
    }
}
