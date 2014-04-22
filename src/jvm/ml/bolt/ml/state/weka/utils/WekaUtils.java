package bolt.ml.state.weka.utils;

import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayesUpdateable;
import weka.classifiers.functions.Logistic;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.classifiers.functions.SGD;
import weka.classifiers.functions.SMO;
import weka.classifiers.lazy.IBk;
import weka.classifiers.lazy.LWL;
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.HoeffdingTree;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.RandomForest;
import weka.clusterers.*;
import weka.core.Attribute;
import weka.core.OptionHandler;
import weka.filters.AllFilter;

import java.text.MessageFormat;
import java.util.ArrayList;

public class WekaUtils {

    public static final String CLASSES_ATTR_NAME = "classes";
    public static final String FEATURE_PREFIX = "feature-";

    public static ArrayList<Attribute> makeFeatureVectorForOnlineClustering(int noOfClusters, int noOfAttributes) {
        // Declare FAST VECTOR
        ArrayList<Attribute> attributeInfo = new ArrayList<Attribute>();

        // Declare FEATURES and add them to FEATURE VECTOR
        for (int i = 0; i < noOfAttributes; i++)
            attributeInfo.add(new Attribute(MessageFormat.format("feature-{0}", i)));

        System.err.println("DEBUG: no. of attributes = " + attributeInfo.size());
        return attributeInfo;
    }

    public static ArrayList<Attribute> makeFeatureVectorForBatchClustering(int noOfAttributes, int numClasses) {
        // Declare FAST VECTOR
        ArrayList<Attribute> attributeInfo = new ArrayList<Attribute>();

        // Declare FEATURES and add them to FEATURE VECTOR
        for (int i = 0; i < noOfAttributes; i++)
            attributeInfo.add(new Attribute(MessageFormat.format("feature-{0}", i)));

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
        Attribute classes = new Attribute(CLASSES_ATTR_NAME, classNames);
        // last element in a FEATURE VECTOR is the category
        attributeInfo.add(classes);
        return attributeInfo;
    }

    public static Classifier makeClassifier(String wekaClassifier, String[] options) throws Exception {
        switch (WekaClassificationAlgorithms.valueOf(wekaClassifier)) {
            case decisionTree:
                J48 j48 = new J48();
                setOptionsForWekaPredictor(options, j48);
                return j48;
            case svm:
                SMO smo = new SMO();
                setOptionsForWekaPredictor(options, smo);
                return smo;
            case logisticRegression:
                Logistic logistic = new Logistic();
                setOptionsForWekaPredictor(options, logistic);
                return logistic;
            case randomForest:
                RandomForest forest = new RandomForest();
                setOptionsForWekaPredictor(options, forest);
                return forest;
            case decisionStump:
                DecisionStump stump = new DecisionStump();
                setOptionsForWekaPredictor(options, stump);
                return stump;
            case perceptron:
                MultilayerPerceptron perceptron = new MultilayerPerceptron();
                setOptionsForWekaPredictor(options, perceptron);
                return perceptron;
            default:
                return new SMO();
        }
    }

    public static Classifier makeOnlineClassifier(String wekaClassifier, String[] options) throws Exception{
        switch (WekaOnlineClassificationAlgorithms.valueOf(wekaClassifier)) {
            case naiveBayes:
                NaiveBayesUpdateable naiveBayesUpdateable = new NaiveBayesUpdateable();
                setOptionsForWekaPredictor(options, naiveBayesUpdateable);
                return naiveBayesUpdateable;
            case locallyWeightedLearner:
                LWL lwl = new LWL();
                setOptionsForWekaPredictor(options, lwl);
                return lwl;
            case nearestNeighbors:
                IBk ibk = new IBk();
                setOptionsForWekaPredictor(options, ibk);
                return ibk;
            case onlineDecisionTree:
                HoeffdingTree tree = new HoeffdingTree();
                setOptionsForWekaPredictor(options, tree);
                return tree;
            case stochasticGradientDescent:
                SGD sgd = new SGD();
                setOptionsForWekaPredictor(options, sgd);
                return sgd;
            default:
                return new NaiveBayesUpdateable();
        }
    }

    public static Clusterer makeClusterer(String wekaClassifier, int numClusters, String[] options) throws Exception {
        try {
            switch (WekaClusterers.valueOf(wekaClassifier)) {
                case kmeans:
                    SimpleKMeans kmeans = new SimpleKMeans();
                    kmeans.setNumClusters(numClusters);
                    setOptionsForWekaPredictor(options, kmeans);
                    return kmeans;
                case densityBased:
                    MakeDensityBasedClusterer clusterer = new MakeDensityBasedClusterer();
                    clusterer.setNumClusters(numClusters);
                    setOptionsForWekaPredictor(options, clusterer);
                    return clusterer;
                case farthestFirst:
                    FarthestFirst ff = new FarthestFirst();
                    ff.setNumClusters(numClusters);
                    setOptionsForWekaPredictor(options, ff);
                    return ff;
                case hierarchicalClusterer:
                    HierarchicalClusterer hc = new HierarchicalClusterer();
                    hc.setNumClusters(numClusters);
                    setOptionsForWekaPredictor(options, hc);
                    return hc;
                case em:
                    EM em = new EM();
                    em.setMaxIterations(10);
                    em.setMaximumNumberOfClusters(numClusters);
                    em.setNumClusters(numClusters);
                    setOptionsForWekaPredictor(options, em);
                    return em;
                case filteredClusterer:
                    kmeans = new SimpleKMeans();
                    kmeans.setNumClusters(numClusters);
                    FilteredClusterer fc = new FilteredClusterer();
                    fc.setFilter(new AllFilter());
                    fc.setClusterer(kmeans);
                    setOptionsForWekaPredictor(options, fc);
                    return fc;
                default:
                    kmeans = new SimpleKMeans();
                    kmeans.setNumClusters(numClusters);
                    setOptionsForWekaPredictor(options, kmeans);
                    return kmeans;
            }
        } catch (Exception e) {
            throw new Exception("Could not make Clusterer", e);
        }
    }

    public static void setOptionsForWekaPredictor(String[] options, OptionHandler kmeans) throws Exception {
        if (options != null) kmeans.setOptions(options);
    }
}
