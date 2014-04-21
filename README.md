mlstorm - Machine Learning in storm ecosystem
---------------------------------------------

Experimenting with parallel streaming PCA and Ensemble methods for collaborative learning.

Introduction
------------
a. A basic description of Storm and its capabilities is available at http://storm.incubator.apache.org/
b. A detailed tutorial on how to install and run a multi-node storm cluster is described at http://www.michael-noll.com/tutorials/running-multi-node-storm-cluster/
c. The following page helps you understand the lifecycle of a storm topology.
   https://github.com/nathanmarz/storm/wiki/Lifecycle-of-a-topology
d. https://github.com/nathanmarz/storm/wiki/Trident-API-Overview provides a detailed description of the API's used in this project.



1. All the learning algorithms are implemented in Trident and use external EJML and Weka libraries. All these libraries reside in the /lib directory in the project home directory. Look at m2-pom.xml to get an idea about the project dependencies.

2. The consensus clustering algorithm (topology.weka.EnsembleClusteringTopology) uses 2-level (shallow!) deep-learning technique. We experimented with relabelling and majority voting based schemes without success on a stream. Interested readers are encouraged to look at Vega-Pons & Ruiz-Shulcloper, 2011 for detailed explanation of the techniques.

3. There are also implementations of an ensemble of binary classifiers (topology.weka.EnsembleBinaryClassifierTopology) and it's online counterpart (topology.weka.OnlineEnsembleBinaryClassifierTopology). All the base/weak learning algorithms are run in parallel with their predictions reduced into (aggregation) a meta-classifier training sample labelled using the original dataset.

4. The Kmeans clustering implementation allows querying different partitions (each partition runs a separate k-means instance). The result of such a query is a partitionId and the query result (for ex. the centroids of all the clusters or the distribution depicting the association of a test sample (feature vector) to the different clusters). Using the partion id returned and the usefulness of the results a human/machine can update the parameters of the model on the fly. The following is an example.

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
