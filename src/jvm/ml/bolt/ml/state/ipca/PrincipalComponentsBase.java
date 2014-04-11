package bolt.ml.state.ipca;

import org.ejml.data.DenseMatrix64F;
import org.ejml.factory.DecompositionFactory;
import org.ejml.factory.SingularValueDecomposition;
import org.ejml.ops.CommonOps;
import org.ejml.ops.NormOps;
import org.ejml.ops.SingularOps;
import spout.dbutils.SensorDbUtils;
import storm.trident.state.State;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by lbhat@DaMSl on 12/22/13.
 * <p/>
 * Copyright {2013} {Lakshmisha Bhat <laksh85@gmail.com>}
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


public abstract class PrincipalComponentsBase implements State {

    protected final int numExpectedComponents;
    protected Map<String, Double> currentSensors;
    protected Map<Long, Map<String, Double>> windowTimesteps;
    protected Map<Integer, String> reverseSensorDictionary;
    protected Map<String, Integer> sensorDictionary;

    // principal component subspace is stored in the rows
    private DenseMatrix64F V_t;

    // how many principal components are used
    private int numPrincipalComponents;

    // state partition information
    private int localPartition;
    private int numPartition;
    protected int windowSize;

    // where the data is stored
    protected DenseMatrix64F A = new DenseMatrix64F(1, 1);
    protected int sampleIndex;

    // mean values of each element across all the samples
    double mean[];

    public PrincipalComponentsBase(final int elementsInSample,
                                   int numPrincipalComponents,
                                   int localPartition,
                                   int numPartitions) throws SQLException {
        this.localPartition = localPartition;
        this.numPartition = numPartitions;
        this.windowSize = elementsInSample;
        this.numExpectedComponents = numPrincipalComponents;
        this.currentSensors = new ConcurrentSkipListMap<String, Double>();
        this.windowTimesteps = new LinkedHashMap<Long, Map<String, Double>>(this.windowSize, 0.75f, false) {
            public boolean removeEldestEntry(Map.Entry<Long, Map<String, Double>> eldest) {
                return size() > windowSize;
            }
        };
        this.windowTimesteps = Collections.synchronizedMap(this.windowTimesteps);
        this.reverseSensorDictionary = new ConcurrentSkipListMap<Integer, String>();
        this.sensorDictionary = new ConcurrentSkipListMap<String, Integer>();
        // Bad practice, but gets the job done :D
        this.reverseSensorDictionary = SensorDbUtils.buildBiDirectionalSensorDictionary(this.sensorDictionary);
    }

    public int getNumOfPrincipalComponents() {
        return this.numPrincipalComponents;
    }

    /**
     * Must be called before any other functions. Declares and sets up internal data structures.
     *
     * @param numSamples Number of samples that will be processed.
     * @param sampleSize Number of elements in each sample.
     */
    public void constructDataMatrixForPca(int numSamples, int sampleSize) {
        this.mean = new double[sampleSize];
        this.A.reshape(numSamples, sampleSize, false);
        this.sampleIndex = 0;
        this.numPrincipalComponents = -1;
    }

    /**
     * Adds a new sample of the raw data to internal data structure for later processing.  All the samples
     * must be added before computeBasis is called.
     *
     * @param sampleData Sample from original raw data.
     */
    public void addSample(double[] sampleData) {
        if (A.getNumCols() != sampleData.length)
            throw new IllegalArgumentException("Unexpected sample size");
        if (sampleIndex >= A.getNumRows())
            throw new IllegalArgumentException("Too many samples");

        for (int i = 0; i < sampleData.length; i++) {
            A.set(sampleIndex, i, sampleData[i]);
        }
        sampleIndex++;
    }

    /**
     * Computes a basis (the principal components) from the most dominant eigenvectors.
     *
     * @param numComponents Number of vectors it will use to describe the data.  Typically much
     *                      smaller than the number of elements in the input vector.
     */
    public void computeBasis(int numComponents) {

        System.out.println("DEBUG: Compute basis to get principal components. ");

        if (numComponents > A.getNumCols())
            throw new IllegalArgumentException("More components requested that the data's length.");
        if (sampleIndex != A.getNumRows())
            throw new IllegalArgumentException("Not all the data has been added");
        if (numComponents > sampleIndex)
            throw new IllegalArgumentException("More data needed to compute the desired number of components " +
                    "(sampleIndex=" + sampleIndex + ") (numComponents = " + numComponents + ") ");

        this.numPrincipalComponents = numComponents;

        // compute the mean of all the samples
        for (int i = 0; i < A.getNumRows(); i++) {
            for (int j = 0; j < mean.length; j++) {
                mean[j] += A.get(i, j);
            }
        }
        for (int j = 0; j < mean.length; j++) {
            mean[j] /= A.getNumRows();
        }

        // subtract the mean from the original data
        for (int i = 0; i < A.getNumRows(); i++) {
            for (int j = 0; j < mean.length; j++) {
                A.set(i, j, A.get(i, j) - mean[j]);
            }
        }

        // Compute SVD and save time by not computing U
        SingularValueDecomposition<DenseMatrix64F> svd =
                DecompositionFactory.svd(A.numRows, A.numCols, false, true, false);
        if (!svd.decompose(A))
            throw new RuntimeException("SVD failed");

        V_t = svd.getV(null, true);
        DenseMatrix64F W = svd.getW(null);

        // Singular values are in an arbitrary order initially
        SingularOps.descendingOrder(null, false, W, V_t, true);

        // strip off unneeded components and find the basis
        V_t.reshape(numComponents, mean.length, true);
    }

    /**
     * Returns all the principal components of data matrix A
     *
     * @return pca
     */
    public synchronized Double[][] getPrincipalComponents() {
        int component = 0;
        final Double[][] eigenRowMajor;
        final int numPrincipalComponents = this.getNumOfPrincipalComponents();
        final int numSensors = this.getReverseSensorDictionary().size();
        eigenRowMajor = new Double[numSensors][numPrincipalComponents];

        while (component < numPrincipalComponents) {
            final double[] basisColumnVector = this.getBasisVector(component);
            for (int sensorIndex = 0; sensorIndex < basisColumnVector.length; sensorIndex++)
                eigenRowMajor[sensorIndex][component] = basisColumnVector[sensorIndex];
            component++;
        }

        return eigenRowMajor;
    }

    /**
     * Returns a vector from the PCA's basis.
     *
     * @param which Which component's vector is to be returned.
     * @return Vector from the PCA basis.
     */
    public double[] getBasisVector(int which) {
        if (which < 0 || which >= numPrincipalComponents)
            throw new IllegalArgumentException("Invalid component");

        DenseMatrix64F v = new DenseMatrix64F(1, A.numCols);
        CommonOps.extract(V_t, which, which + 1, 0, A.numCols, v, 0, 0);

        return v.data;
    }

    /**
     * Converts a vector from sample space into eigen space.
     *
     * @param sampleData Sample space data.
     * @return Eigen space projection.
     */
    public double[] sampleToEigenSpace(double[] sampleData) {
        if (sampleData.length != A.getNumCols())
            throw new IllegalArgumentException("Unexpected sample length");
        DenseMatrix64F mean = DenseMatrix64F.wrap(A.getNumCols(), 1, this.mean);

        DenseMatrix64F s = new DenseMatrix64F(A.getNumCols(), 1, true, sampleData);
        DenseMatrix64F r = new DenseMatrix64F(numPrincipalComponents, 1);

        CommonOps.sub(s, mean, s);

        CommonOps.mult(V_t, s, r);

        return r.data;
    }

    /**
     * Converts a vector from eigen space into sample space.
     *
     * @param eigenData Eigen space data.
     * @return Sample space projection.
     */
    public double[] eigenToSampleSpace(double[] eigenData) {
        if (eigenData.length != numPrincipalComponents)
            throw new IllegalArgumentException("Unexpected sample length");

        DenseMatrix64F s = new DenseMatrix64F(A.getNumCols(), 1);
        DenseMatrix64F r = DenseMatrix64F.wrap(numPrincipalComponents, 1, eigenData);

        CommonOps.multTransA(V_t, r, s);

        DenseMatrix64F mean = DenseMatrix64F.wrap(A.getNumCols(), 1, this.mean);
        CommonOps.add(s, mean, s);

        return s.data;
    }


    /**
     * <p>
     * The membership error for a sample.  If the error is less than a threshold then
     * it can be considered a member.  The threshold's value depends on the data set.
     * </p>
     * <p>
     * The error is computed by projecting the sample into eigenspace then projecting
     * it back into sample space and
     * </p>
     *
     * @param sampleA The sample whose membership status is being considered.
     * @return Its membership error.
     */
    public double errorMembership(double[] sampleA) {
        double[] eig = sampleToEigenSpace(sampleA);
        double[] reproj = eigenToSampleSpace(eig);


        double total = 0;
        for (int i = 0; i < reproj.length; i++) {
            double d = sampleA[i] - reproj[i];
            total += d * d;
        }

        return Math.sqrt(total);
    }

    /**
     * Computes the dot product of each basis vector against the sample.  Can be used as a measure
     * for membership in the training sample set.  High values correspond to a better fit.
     *
     * @param sample Sample of original data.
     * @return Higher value indicates it is more likely to be a member of input dataset.
     */
    public double response(double[] sample) {
        if (sample.length != A.numCols)
            throw new IllegalArgumentException("Expected input vector to be in sample space");

        DenseMatrix64F dots = new DenseMatrix64F(numPrincipalComponents, 1);
        DenseMatrix64F s = DenseMatrix64F.wrap(A.numCols, 1, sample);

        CommonOps.mult(V_t, s, dots);
        return NormOps.normF(dots);
    }




    /**
     * Returns the latest map of sensor data in the window
     *
     * @return map of sensors and data
     */
    public Map<String, Double> getCurrentSensors() {
        return getCurrentSensorsAndReset(false);
    }


    /**
     * Internal
     *
     * @param reset
     * @return
     */
    protected synchronized Map<String, Double> getCurrentSensorsAndReset(final boolean reset) {
        final Map<String, Double> oldSensors = currentSensors;
        if (reset) currentSensors = new ConcurrentSkipListMap<String, Double>();
        return oldSensors;
    }

    /**
     * Returns the local partition Id
     *
     * @return
     */
    public int getLocalPartition() {
        return localPartition;
    }

    /**
     * Returns the total number of partitions persisting this state
     *
     * @return
     */
    public int getNumPartition() {
        return numPartition;
    }

    /**
     * Returns the dictionary that maps sensor ids to names
     *
     * @return Map
     */
    public Map<Integer, String> getReverseSensorDictionary() {
        return reverseSensorDictionary;
    }
}
