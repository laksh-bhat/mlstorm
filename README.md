mlstorm
=======

Machine Learning in storm: Experimenting with parallel streaming PCA and Ensemble methods for collaborative learning.
--------------------------------------------------------------------------------------------------------------------------

1. The PCA and Clustering algorithm implementations are window based. There's also an online/incremental PCA implementation available for experimentation.

2. All the learning algorithms are implemented in Trident and use external EJML and Weka libraries.

3. The consensus clustering algorithms (EnsembleClusteringTopology.java) use 2-level (shallow!) deep-learning technique. I experimented with relabelling and majority voting based scheme without decent result on a stream. Interested readers are encouraged to look at Vega-Pons & Ruiz-Shulcloper, 2011 for detailed explanation of the techniques.  

4. There are also an implementation of EnsembleBinaryClassifierTopology and it's online counterpart in OnlineEnsembleBinaryClassifierTopology.java. All the base algorithms are run in parallel with intelligent reduction (aggregation) into meta-classifier inputs. 

5. The Kmeans clustering implementation allows querying different partitions. The result of such a query is a partitionId and the query result. Using the partion id returned and the usefulness of the results a human/machine can update the parameters of the model on the fly. The following is an example.

==========================================================================================================================
       a. The topology is run as usual.
       
         storm jar /damsl/software/storm/code/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar topology.weka.KmeansClusteringTopology /damsl/projects/bpti_db/features<directory containing feature vectors> 4<no of workers> 10000 10<k> 10<parallelism>

==========================================================================================================================

       b. A distributed query (querying for parameters/model statistics) on the model can be executed like the following.
         java -cp .: (output of "storm classpath" command) : $REPO/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar drpc.DrpcQueryRunner qp-hd3 kmeans "no args" *-- This returns the centroids of all the clusters for a given K --*
==========================================================================================================================
       c. An update could be made like the following.
       
	 java -cp .:`storm classpath`:$REPO/mlstorm/target/mlstorm-00.01-jar-with-dependencies.jar drpc.DrpcQueryRunner qp-hd3 kUpdate 0,45
	 
	 Description: In (c) we are updating the parameters of the said algorithm (K-means clustering) on the fly using DRPC. We started the topology with 10 partitions and (c) is updating the Clusterer at partition '0' to [k=45]. 
	 
         Result : <[["0,35","k update request (30->35) received at [0]; average trainingtime for k = [30] = [334,809]ms"]]>

       The classpath above is returned by the "storm classpath" command.
==========================================================================================================================
       
       
