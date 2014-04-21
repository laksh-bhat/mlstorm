#mlstorm - Machine Learning in Storm Ecosystem

Experimenting with online ensemble methods for collaborative learning and parallel streaming principal components analysis.

## Introduction

a. A basic description of Storm and its capabilities is available at [storm home page] (http://storm.incubator.apache.org/)

b. A detailed tutorial on how to install and run a multi-node storm cluster is available [here] ( http://www.michael-noll.com/tutorials/running-multi-node-storm-cluster/)

c. This [wiki page] (https://github.com/nathanmarz/storm/wiki/Lifecycle-of-a-topology) helps you understand the lifecycle of a storm topology

d. This [wiki page] (https://github.com/nathanmarz/storm/wiki/Trident-API-Overview) provides a detailed description of the API's used in this project.

e. This [wiki page] (https://github.com/nathanmarz/storm/wiki/Understanding-the-parallelism-of-a-Storm-topology) should be useful to understand the parallelism of a storm topology.

f. The common configurations and details of running Storm on a production cluster can be found in this [wiki page] (https://github.com/nathanmarz/storm/wiki/Running-topologies-on-a-production-cluster)

g. We have implemented spouts to stream BPTI features, sensor data etc. spout.mddb.MddbFeatureExtractorSpout, spout.sensor.SensorStreamingSpout and spout.AustralianElectricityPricingSpout are all NonTransactional spouts. A detailed description of Transactional, Non-Transactional and Opaque-Transactional spouts is available [here] (https://github.com/nathanmarz/storm/wiki/Trident-spouts)

## A General Framework for Online Machine Learning In Storm


Our framework integrates the Storm stream processing system with the ability to perform exploratory and confirmatory data analysis tasks through the WEKA toolkit. WEKA is a popular library for a range of data mining and machine learning algorithms implemented in the Java programming language. As such, it is straightforward to incorporate WEKA’s algorithms directly into Storm’s bolts (where bolts are the basic unit of processing in the Storm system). 

WEKA is designed as a general-purpose data mining and machine learning library, with its typical use-case in offline modes, where users have already collected and curated their dataset, and are now seeking to determine relationships in the dataset through analysis. To enable WEKA’s algorithms to be used in an online fashion, we decouple the continuous arrival of stream events from the execution of WEKA algorithms through window-based approaches. Our framework supplies window capturing facilities through Storm’s provision of stateful topology components, for example its State classes, persistentAggregate, and stateQuery topology construction methods to access and manipulate state. Our windows define the scope of the data on which we can evaluate WEKA algorithms. Furthermore, while we have implemented basic window sliding capabilities, one can arbitrarily manage the set of open windows in the system through Storm’s state operations. This allows us to dynamically select which windows we should consider for processing, beyond standard sliding mechanisms, for example disjoint windows or predicate-based windows that are defined by other events in the system.

In our framework, each window is supplied for training to a WEKA analysis algorithm. Once training completes, the resulting model can be used to predict or classify future stream events arriving to the system. Below, we present our framework as used with the k-means clustering and principal components analysis algorithms. Our current focus is to support the scalable exploration, training and validation performed by multiple algorithms simultaneously in the Storm system, as is commonly needed when the class of algorithms that ideally models a particular dataset is not known up front, and must be determined through experimentation. For example, with our framework, we can run multiple k-means clustering algorithms in Storm, each configured with a different parameter value for k. Thus in many application scenarios where k is not known a priori, we can discover k through experimentation. Our framework leverages Storm’s abilities to distribute its program topology across multiple machines for scalable execution of analysis algorithms, as well as its fault-tolerance features. 


### Implementation Details 

1. All the learning algorithms are implemented in Trident and use external EJML and Weka libraries. All these libraries reside in the /lib directory in the project home directory. Look at m2-pom.xml to get an idea about the project dependencies.

2. The consensus clustering algorithm (topology.weka.EnsembleClusteringTopology) uses 2-level (shallow!) deep-learning technique. We experimented with relabelling and majority voting based schemes without success on a stream. Interested readers are encouraged to look at Vega-Pons & Ruiz-Shulcloper, 2011 (A Survey of Clustering Ensemble Algorithms) and Wei-Hao Lin, Alexander Hauptmann, 2003 (Meta-classification: Combining Multimodal Classifiers) for detailed explanation of the techniques.

### Experiments

2. Our implementation of consensus clustering uses the MddbFeatureExtractorSpout to inject feature vectors. All the base clusterers we use implement [weka.clustereres interface] (http://weka.sourceforge.net/doc.dev/weka/clusterers/Clusterer.html). The ensemble consists of SimpleKMeans, DensityBasedClusterer, FarthestFirst, HierarchicalClusterer, EM and FilteredClusterer with base algorithm being SimpleKMeans. The meta-clustering algorithm is density based. If you want modify/replace these algorithms, you may do so by updating the Enum class - bolt.ml.state.weka.utils.WekaClusterers and the factory methods in bolt.ml.state.weka.utils.WekaUtils. We use default parameters for individual clusterers but you may inject the appropriate options that can be handled by weka OptionHandler. For example, you can find all the available options for [SimpleKMeans] ( http://weka.sourceforge.net/doc.dev/weka/clusterers/SimpleKMeans.html), which could be specified as a Java String[]. 

  a. To submit the topology one can fire away the following command.

      <code> storm jar $REPO/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar topology.weka.EnsembleClustererTopology /damsl/projects/bpti_db/features 4 1000 5 1 </code>
      
    The arguments to the topology is described below
  
     1. <code>directory - The folder containing containing feature vectors (We use BPTI dataset for our experiments. The spout  implementation - spout.mddb.MddbFeatureExtractorSpout - is responsible to feed the topology with the feature vectors. Look at res/features_o.txt for an example dataset. If you want access to the bpti dataset, contact lbhat1@jhu.edu or yanif@jhu.edu)</code>
     2. <code> no of workers - total no. of nodes to run the topology on. </code>
     3. <code> window-size - The total number of training examples in the sliding window </code>
     4. <code> k - the number of clusters </code>
     5. <code> parallelism - the number of threads per bolt </code>
  

  b. To predict the most likely cluster for a test sample, one can invoke a drpc query to query the meta learner state.
  
    <code> java -cp .:`storm classpath`:$REPO/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar drpc.BptiEnsembleQuery drpc-server-host ClustererEnsemble </code>
    
    <bold> Note that the second argument to the DRPC query specifies the drpc function to invoke. These names are hard-coded in our Java files (topology.weka.EnsembleClassifierTopology.java and so on.)</bold>

3. There is also an implementation of an ensemble of binary classifiers (topology.weka.EnsembleBinaryClassifierTopology) and it's online counterpart (topology.weka.OnlineEnsembleBinaryClassifierTopology). All the base/weak learning algorithms are run in parallel with their predictions reduced into (ReducerAggregator) a meta-classifier training sample labelled using the original dataset. We use linear SVM as our meta classifier and a bunch of weak learners including pruned decision trees, perceptron, svm, decision stubs etc.

    You may submit the topologies for execution by the following command

      <code> storm jar $REPO/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar topology.weka.EnsembleClassifierTopology $REPO/mlstorm/res/elecNormNew.arff 1 1000 5 1 </code>
      
      <code> storm jar $REPO/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar topology.weka.OnlineEnsembleClassifierTopology $REPO/mlstorm/res/elecNormNew.arff 1 1000 5 1 </code>
      
    
    To classify a test example, one can invoke a drpc query to query the meta learner (SVM, by default) state.
      
      <code> java -cp .:`storm classpath`:$REPO/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar drpc.AustralianElectricityPricingQuery drpc-server-host-name ClassifierEnsemble </code>
      
      <code> java -cp .:`storm classpath`:$REPO/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar drpc.AustralianElectricityPricingQuery drpc-server-host-name OnlineClassifierEnsemble </code>
      
      

4. Our implementation of Kmeans clustering allows querying different partitions (each partition runs a separate k-means instance). The result of such a query is a partitionId and the query result (for ex. the centroids of all the clusters or the distribution depicting the association of a test sample (feature vector) to the different clusters). Using the partion id returned and the usefulness of the results a human/machine can update the parameters of the model on the fly. The following is an example.

  a.  Submit/Start the topology as usual.

      storm jar /damsl/software/storm/code/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar topology.weka.KmeansClusteringTopology /damsl/projects/bpti_db/features 4 10000 10 10

         
   The arguments to the topology is described below:
   1. <code> directory -- The folder containing containing feature vectors </code>
   2. <code> no of workers -- total no. of nodes to run the topology on. </code>
   3. <code> k -- the number of clusters </code>
   4. <code> parallelism -- the number of threads per bolt </code>


  b. A distributed query (querying for parameters/model statistics) on the model can be executed as below. This query returns the centroids of all the clusters for a given instance of clustering algorithm.

      <code>java -cp .: `storm classpath` : $REPO/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar drpc.DrpcQueryRunner qp-hd3 kmeans "no args" </code>

  c. A parameter update (k for k-means) can be made using the following DRPC query. Here we are updating the parameters of the said algorithm (K-means clustering) on the fly using DRPC. We started the topology with 10 partitions and (c) is updating the Clusterer at partition '0' to [k=45]. 

      <code>java -cp .:`storm classpath`:$REPO/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar  drpc.DrpcQueryRunner qp-hd3 kUpdate 0,45 </code>

    The result of the above query looks like the following:

      <[["0,35","k update request (30->35) received at [0]; average trainingtime for k = [30] = [334,809]ms"]]>
       
       
5. The PCA and Clustering algorithm implementations are window based. There's also an online/incremental PCA implementation available for experimentation.


###Storm Experience Report

Storm is an open-source system that was initially developed by BackType before its acquisition by Twitter, Inc. The documentation and support for Storm primarily arises from the open-source user community that continues to develop its capabilities through the github project repository. Storm has been incubated as an Apache project as of September 2013. In this section we provide an initial report on our experiences in developing with the Storm framework, particularly as we deploy it onto a cluster environment and develop online learning and event detection algorithms.

##### Notes useful for Debugging
##### Storm Components

###### supervisor
  - JVM process launched on each storm worker machine. This does not execute your code — just supervises it.
  - number of workers is set by number of supervisor.slots.ports on each machine.
  
###### worker
  - jvm process launched by the supervisor
  - intra-worker transport is more efficient, so we run one worker per topology per machine
  - if worker dies, supervisor will restart. But worker dies for any minor failure (fail fast)
  
###### Coordinator 
  - generates new transaction ID
  - sends tuple, which influences spout to dispatch a new batch
  - each transaction ID corresponds identically to single trident batch and vice-versa
  - Transaction IDs for a given topo_launch are serially incremented globally.
  - knows about Zookeeper/transactional; so it recovers the transaction ID.
  
###### Executor
  - Each executor is responsible for one bolt or spout.
  - therefore with 3 sensor/MDDB spouts on a worker, there are three executors spouting.

###### Hard to Debug Mistakes
  - A spout must never block when emitting — if it blocks, critical bookkeeping tuples will get trapped, and the topology hangs. So its emitter keeps an "overflow buffer", and publishes as follows:
    - if there are tuples in the overflow buffer add the tuple to it — the queue is certainly full.
    - otherwise, publish the tuple to the flow with the non-blocking call. That call will either succeed immediately
      or fail with an InsufficientCapacityException, in which case add the tuple to the overflow buffer.
    - The spout’s async-loop won’t call nextTuple if overflow is present, so the overflow buffer only has to accommodate the maximum number of tuples emitted in a single nextTuple call.

##### Acking
  - Acker is just a regular bolt — all the interesting action takes place in its execute method.
  - set number of ackers equal to number of workers. (default is 1 per topology)
  - The acker holds a single O(1) lookup table
  - it is actually a collection of lookup tables: current, old and dead. new tuple trees are added to the current bucket; after every timeout number of seconds, current becomes old, and old becomes dead — they are declared failed and their records retried.
  - it knows id == tuple[0] the tuple’s stream-id
  - there is a time-expiring data structure, the RotatingHashMap
  - when you go to update or add to it, it performs the operation on the right component of HashMap.
  - periodically (when you receive a tick tuple in Storm8.2+), it will pull off oldest component HashMap, mark it as dead; invoke the expire callback for each element in that HashMap.

##### Throttling
  - Max spout pending (TOPOLOGY_MAX_SPOUT_PENDING) sets the number of tuple trees live in the system at any point in time.
  - Trident batch emit interval (topology.trident.batch.emit.interval.millis) sets the maximum pace at which the trident master batch co-ordinator issues new seed tuples. If batch delay is 500ms and the most recent batch was released 486ms, the spout coordinator will wait 14ms before dispensing a new seed tuple. If the next pending entry isn’t cleared for 523ms, it will be dispensed immediately.
  - Trident batch emit interval  is extremely useful to prevent congestion, especially around startup/rebalance. 
  - As opposed to a traditional Storm spout, a Trident spout will likely dispatch hundreds of records with each batch. If max-pending is 20, and the spout releases 500 records per batch, the spout will try to cram 10,000 records into its send queue.

##### Batch Size
  - Set the batch size to optimize the throughput of the most expensive batch operation — a bulk database operation, network request, or large aggregation.
  - When the batch size is too small, bookkeeping dominates response time i.e response time is constant
  - Execution times increase slowly and we get better and better records-per-second throughput with increase in batch size.
  - at some point, we start overwhelming some resource and execution time increases sharply (usually due to network failures and replays in our case)
 

