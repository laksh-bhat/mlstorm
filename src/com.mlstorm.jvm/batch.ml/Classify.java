import classifier.Predictor;
import classifier.supervisedlearning.decisiontreetrainer.DecisionTree;
import classifier.supervisedlearning.ensembles.EnsembleFeatureBaggingPredictor;
import classifier.supervisedlearning.ensembles.EnsembleInstanceBaggingPredictor;
import classifier.supervisedlearning.generalizedlearningmodels.NaiveBayesPredictor;
import classifier.supervisedlearning.generalizedlearningmodels.PerceptronPredictor;
import classifier.supervisedlearning.generalizedlearningmodels.WinnowPredictor;
import classifier.supervisedlearning.knn.DistanceWeightedKNNPredictor;
import classifier.supervisedlearning.knn.SimpleKNNPredictor;
import classifier.supervisedlearning.simpleclassifier.EvenOddClassifier;
import classifier.supervisedlearning.simpleclassifier.MajorityClassifier;
import classifier.supervisedlearning.svm.kernel.GaussianKernelLogisticRegression;
import classifier.supervisedlearning.svm.kernel.LinearKernelLogisticRegression;
import classifier.supervisedlearning.svm.kernel.PolynomialKernelLogisticRegression;
import classifier.unsupervisedlearning.LambdaMeansPredictor;
import dataobject.Instance;
import dataobject.label.Label;
import evaluate.AccuracyEvaluator;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import utils.CommandLineUtilities;
import utils.DataReader;
import utils.PredictionsWriter;

import java.io.*;
import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;

public class Classify {
    static public LinkedList<Option> options = new LinkedList<Option>();
    private static Predictor predictor;

    private static int computeNumberOfInstances(List<Instance> instances) {
        int noOfInstances = 0;
        for (Instance instance : instances) {
            if (instance.getLabel() != null) {
                noOfInstances++;
            }
        }
        return noOfInstances;
    }

    private static Predictor constructPredictorBaseOnAlgorithm(String algorithm) {
        Predictor predictor = null;
        if (algorithm.equalsIgnoreCase("majority")) {
            predictor = new MajorityClassifier();
        } else if (algorithm.equalsIgnoreCase("even_odd")) {
            predictor = new EvenOddClassifier();
        } else if (algorithm.equalsIgnoreCase("decision_tree")) {
            predictor = new DecisionTree();
        } else if (algorithm.equalsIgnoreCase("naive_bayes")) {
            predictor = new NaiveBayesPredictor();
        } else if (algorithm.equalsIgnoreCase("perceptron")) {
            predictor = new PerceptronPredictor();
        } else if (algorithm.equalsIgnoreCase("winnow")) {
            predictor = new WinnowPredictor();
        } else if (algorithm.equalsIgnoreCase("logistic_regression_linear_kernel")) {
            predictor = new LinearKernelLogisticRegression();
        } else if (algorithm.equalsIgnoreCase("logistic_regression_polynomial_kernel")) {
            predictor = new PolynomialKernelLogisticRegression();
        } else if (algorithm.equalsIgnoreCase("logistic_regression_gaussian_kernel")) {
            predictor = new GaussianKernelLogisticRegression();
        } else if (algorithm.equalsIgnoreCase("knn")) {
            predictor = new SimpleKNNPredictor();
        } else if (algorithm.equalsIgnoreCase("knn_distance")) {
            predictor = new DistanceWeightedKNNPredictor();
        } else if (algorithm.equalsIgnoreCase("instance_bagging")) {
            predictor = new EnsembleInstanceBaggingPredictor();
        } else if (algorithm.equalsIgnoreCase("feature_bagging")) {
            predictor = new EnsembleFeatureBaggingPredictor();
        } else if (algorithm.equalsIgnoreCase("lambda_means")) {
            predictor = new LambdaMeansPredictor();
        }
        return predictor;
    }

    private static void createCommandLineOptions() {
        registerOption("data", "String", true, "The data to use.");
        registerOption("mode", "String", true, "Operating mode: train or test.");
        registerOption("predictions_file", "String", true, "The predictions file to create.");
        registerOption("algorithm", "String", true, "The name of the algorithm for training.");
        registerOption("model_file", "String", true, "The name of the model file to create/load.");
        registerOption("max_decision_tree_depth", "int", true, "The maximum depth of the decision tree.");
        registerOption("lambda", "double", true, "The level of smoothing for Naive Bayes.");
        registerOption("thickness", "double", true, "The value of the linear separator thickness.");
        registerOption("online_learning_rate", "double", true, "The LTU learning rate.");
        registerOption("online_training_iterations", "int", true, "The number of training iterations for LTU.");
        registerOption("gradient_ascent_learning_rate", "double", true, "The learning rate for logistic regression.");
        registerOption("polynomial_kernel_exponent", "double", true, "The exponent of the polynomial kernel.");
        registerOption("gradient_ascent_training_iterations", "int", true, "The number of training iterations.");
        registerOption("gaussian_kernel_sigma", "double", true, "The sigma of the Gaussian kernel.");
        registerOption("k_nn", "int", true, "The value of K for KNN regression.");
        registerOption("k_ensemble", "int", true, "The number of classifiers in the ensemble.");
        registerOption("ensemble_learning_rate", "double", true, "The ensemble learning rate.");
        registerOption("cluster_lambda", "double", true, "The value of lambda in lambda-means.");
        registerOption("clustering_training_iterations", "int", true, "The number of lambda-means EM iterations.");
        // Other options will be added here.
    }

    private static void evaluateAccuracyIfLabelsAreAvailable(List<Instance> instances, Predictor predictor) {
        if (predictor instanceof SimpleKNNPredictor || predictor instanceof DistanceWeightedKNNPredictor) {
            System.out.println("Mean error is " + new AccuracyEvaluator().evaluateR(instances, predictor));

        } else if (computeNumberOfInstances(instances) > 0) {
            System.out.println(MessageFormat.format("match percentage is {0}",
                    new AccuracyEvaluator().evaluate(instances, predictor) / instances.size() * 100));
        }
    }

    private static void evaluateAndSavePredictions(
            Predictor predictor,
            List<Instance> instances, String predictions_file
    ) throws IOException {
        PredictionsWriter writer = new PredictionsWriter(predictions_file);
        // Evaluate the model if labels are available.
        evaluateAccuracyIfLabelsAreAvailable(instances, predictor);

        for (Instance instance : instances) {
            Label label = predictor.predict(instance);
            writer.writePrediction(label);
        }

        writer.close();
    }

    /**
     * Load a single object from a filename.
     *
     * @param file_name
     * @return
     */
    public static Object loadObject(String file_name) {
        ObjectInputStream ois;
        try {
            ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(new File(file_name))));
            Object object = ois.readObject();
            ois.close();
            return object;
        } catch (IOException e) {
            System.err.println("Error loading: " + file_name);
        } catch (ClassNotFoundException e) {
            System.err.println("Error loading: " + file_name);
        }
        return null;
    }

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();

        // Parse the command line.
        String[] manditory_args = {"mode"};
        createCommandLineOptions();
        CommandLineUtilities.initCommandLineParameters(args, Classify.options, manditory_args);

        String mode = CommandLineUtilities.getOptionValue("mode");
        String data = CommandLineUtilities.getOptionValue("data");
        String predictions_file = CommandLineUtilities.getOptionValue("predictions_file");
        String algorithm = CommandLineUtilities.getOptionValue("algorithm");
        String model_file = CommandLineUtilities.getOptionValue("model_file");


        if (mode.equalsIgnoreCase("train")) {
            if (data == null || algorithm == null || model_file == null) {
                System.out.println("Train requires the following arguments: data, algorithm, model_file");
                System.exit(0);
            }

            // Load the training data.
            boolean isClassification = true;
            if (algorithm.contains("knn"))  // knn is regression
            {
                isClassification = false;
            }

            DataReader data_reader = new DataReader(data, isClassification);
            List<Instance> instances = data_reader.readData();
            data_reader.close();

            // Train the model.
            Predictor predictor = train(instances, algorithm);
            saveObject(predictor, model_file);

        } else if (mode.equalsIgnoreCase("test")) {
            if (data == null || predictions_file == null || model_file == null) {
                System.out.println("Test requires the following arguments: data, predictions_file, model_file");
                System.exit(0);
            }

            // Load the test data.
            boolean isClassification = true;
            if (algorithm.contains("knn"))  // knn is regression
            {
                isClassification = false;
            }
            DataReader data_reader = new DataReader(data, isClassification);
            List<Instance> instances = data_reader.readData();
            data_reader.close();

            // Load the model.
            Predictor predictor = (Predictor) loadObject(model_file);
            evaluateAndSavePredictions(predictor, instances, predictions_file);
        } else {
            System.out.println("Requires mode argument.");
        }

        long elapsedTimeMillis = System.currentTimeMillis() - start;
        float elapsedTimeSec = elapsedTimeMillis / 1000F;
        System.out.println(MessageFormat.format("Time spent executing = {0}", elapsedTimeSec));
    }

    public static void registerOption(String option_name, String arg_name, boolean has_arg, String description) {
        OptionBuilder.withArgName(arg_name);
        OptionBuilder.hasArg(has_arg);
        OptionBuilder.withDescription(description);
        Option option = OptionBuilder.create(option_name);

        Classify.options.add(option);
    }

    public static void saveObject(Object object, String file_name) {
        try {
            ObjectOutputStream oos =
                    new ObjectOutputStream(new BufferedOutputStream(
                            new FileOutputStream(new File(file_name))));
            oos.writeObject(object);
            oos.close();
        } catch (IOException e) {
            System.err.println("Exception writing file " + file_name + ": " + e);
        }
    }

    private static Predictor train(List<Instance> instances, String algorithm) {
        Predictor predictor = constructPredictorBaseOnAlgorithm(algorithm);
        if (predictor != null) {
            predictor.train(instances);
            evaluateAccuracyIfLabelsAreAvailable(instances, predictor);
        }
        return predictor;
    }
}
