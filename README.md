mlstorm - Machine Learning in storm ecosystem
---------------------------------------------

Experimenting with parallel streaming PCA and Ensemble methods for collaborative learning.

Introduction
------------
a. A basic description of Storm and its capabilities is available at - http://storm.incubator.apache.org/

b. A detailed tutorial on how to install and run a multi-node storm cluster is available at - http://www.michael-noll.com/tutorials/running-multi-node-storm-cluster/

c. The following wiki page helps you understand the lifecycle of a storm topology -  https://github.com/nathanmarz/storm/wiki/Lifecycle-of-a-topology

d. The following wiki provides a detailed description of the API's used in this project - https://github.com/nathanmarz/storm/wiki/Trident-API-Overview

e. This following wiki page should be useful to understand the parallelism of a storm topology - https://github.com/nathanmarz/storm/wiki/Understanding-the-parallelism-of-a-Storm-topology

f. We have implemented several spouts to stream BPTI features, sensor data etc. spout.mddb.MddbFeatureExtractorSpout, spout.sensor.SensorStreamingSpout and spout.AustralianElectricityPricingSpout are all NonTransactional spouts. A detailed description of Transactional, Non-Transactional and Opaque-Transactional spouts is available here - https://github.com/nathanmarz/storm/wiki/Trident-spouts

A General Framework for Online Machine Learning In Storm
---------------------------------------------------------

Our framework integrates the Storm stream processing system with the ability to perform exploratory and confirmatory data analysis tasks through the WEKA toolkit. WEKA is a popular library for a range of data mining and machine learning algorithms implemented in the Java programming language. As such, it is straightforward to incorporate WEKA’s algorithms directly into Storm’s bolts (where bolts are the basic unit of processing in the Storm system). 

WEKA is designed as a general-purpose data mining and machine learning library, with its typical use-case in offline modes, where users have already collected and curated their dataset, and are now seeking to determine relationships in the dataset through analysis. To enable WEKA’s algorithms to be used in an online fashion, we decouple the continuous arrival of stream events from the execution of WEKA algorithms through window-based approaches. Our framework supplies window capturing facilities through Storm’s provision of stateful topology components, for example its State classes, persistentAggregate, and stateQuery [1] topology construction methods to access and manipulate state. Our windows define the scope of the data on which we can evaluate WEKA algorithms. Furthermore, while we have implemented basic window sliding capabilities, one can arbitrarily manage the set of open windows in the system through Storm’s state operations. This allows us to dynamically select which windows we should consider for processing, beyond standard sliding mechanisms, for example disjoint windows or predicate-based windows that are defined by other events in the system.

In our framework, each window is supplied for training to a WEKA analysis algorithm. Once training completes, the resulting model can be used to predict or classify future stream events arriving to the system. Below, we present our framework as used with the k-means clustering and principal components analysis algorithms. Our current focus is to support the scalable exploration, training and validation performed by multiple algorithms simultaneously in the Storm system, as is commonly needed when the class of algorithms that ideally models a particular dataset is not known up front, and must be determined through experimentation. For example, with our framework, we can run multiple k-means clustering algorithms in Storm, each configured with a different parameter value for k. Thus in many application scenarios where k is not known a priori, we can discover k through experimentation. Our framework leverages Storm’s abilities to distribute its program topology across multiple machines for scalable execution of analysis algorithms, as well as its fault-tolerance features. 


Implementation Details 
----------------------

1. All the learning algorithms are implemented in Trident and use external EJML and Weka libraries. All these libraries reside in the /lib directory in the project home directory. Look at m2-pom.xml to get an idea about the project dependencies.

2. The consensus clustering algorithm (topology.weka.EnsembleClusteringTopology) uses 2-level (shallow!) deep-learning technique. We experimented with relabelling and majority voting based schemes without success on a stream. Interested readers are encouraged to look at Vega-Pons & Ruiz-Shulcloper, 2011 for detailed explanation of the techniques.

Experiments
-----------
3. There are also implementations of an ensemble of binary classifiers (topology.weka.EnsembleBinaryClassifierTopology) and it's online counterpart (topology.weka.OnlineEnsembleBinaryClassifierTopology). All the base/weak learning algorithms are run in parallel with their predictions reduced into (aggregation) a meta-classifier training sample labelled using the original dataset.

4. Our implementation of Kmeans clustering allows querying different partitions (each partition runs a separate k-means instance). The result of such a query is a partitionId and the query result (for ex. the centroids of all the clusters or the distribution depicting the association of a test sample (feature vector) to the different clusters). Using the partion id returned and the usefulness of the results a human/machine can update the parameters of the model on the fly. The following is an example.

  a.  Submit/Start the topology as usual.

      storm jar /damsl/software/storm/code/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar topology.weka.KmeansClusteringTopology /damsl/projects/bpti_db/features 4 10000 10 10

         
   The arguments to the topology is described below:
   1. <directory> The folder containing containing feature vectors (We use BPTI dataset for our experiments. The spout  implementation - spout.mddb.MddbFeatureExtractorSpout - is responsible to feed the topology with the feature vectors. Look at res/features_o.txt for an example dataset. If you want access to the bpti dataset, contact lbhat1@jhu.edu or yanif@jhu.edu)
   2. <no of workers> total no. of nodes to run the topology on.
   3. <k> the number of clusters
   4. <parallelism> the number of threads per bolt


  b. A distributed query (querying for parameters/model statistics) on the model can be executed as below. This query returns the centroids of all the clusters for a given instance of clustering algorithm.

      java -cp .: `storm classpath` : $REPO/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar drpc.DrpcQueryRunner qp-hd3 kmeans "no args" 



  c. A parameter update (k for k-means) can be made using the following DRPC query. Here we are updating the parameters of the said algorithm (K-means clustering) on the fly using DRPC. We started the topology with 10 partitions and (c) is updating the Clusterer at partition '0' to [k=45]. 

      java -cp .:`storm classpath`:$REPO/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar  drpc.DrpcQueryRunner qp-hd3 kUpdate 0,45


    The result of the above query looks like the following:

      <[["0,35","k update request (30->35) received at [0]; average trainingtime for k = [30] = [334,809]ms"]]>
       
       
5. The PCA and Clustering algorithm implementations are window based. There's also an online/incremental PCA implementation available for experimentation.
