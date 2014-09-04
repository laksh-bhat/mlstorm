package bolt.ml.state.pca;

import org.ejml.data.DenseMatrix64F;
import org.ejml.factory.DecompositionFactory;
import org.ejml.factory.SingularValueDecomposition;
import org.ejml.ops.CommonOps;
import org.ejml.ops.NormOps;
import org.ejml.ops.SingularOps;
import storm.trident.state.State;
import utils.fields.FieldTemplate;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;

 /*
 * Copyright 2013-2015 Lakshmisha Bhat
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


public abstract class PrincipalComponentsBase implements State {

    protected final int windowSize;
    protected final int numExpectedComponents;
    // state partition information
    private final int localPartition;
    private final int numPartition;
    private final FieldTemplate fieldTemplate;
    protected Map<String, Double> currentSamples;
    protected Map<String, Integer> featureDictionary;
    protected Map<Long, Map<String, Double>> timesteps;
    protected Map<Integer, String> reverseLookupFeatureDictionary;
    protected int sampleIndex;
    // where the data is stored
    private DenseMatrix64F dataMatrix = new DenseMatrix64F(1, 1);
    // mean values of each element across all the samples
    private double[] mean;
    // principal component subspace is stored in the rows
    private DenseMatrix64F principalComponentSubspace;
    // how many principal components are used
    private int numPrincipalComponents;

    public PrincipalComponentsBase(final int numElementsInWindow, int numPrincipalComponents, int localPartition, int numPartitions, FieldTemplate fieldTemplate) throws Exception {
        this.localPartition = localPartition;
        this.numPartition = numPartitions;
        this.windowSize = numElementsInWindow;
        this.numExpectedComponents = numPrincipalComponents;
        this.currentSamples = new ConcurrentSkipListMap<String, Double>();

        // Least recently updated Sliding window
        this.timesteps = new LinkedHashMap<Long, Map<String, Double>>(this.windowSize, 0.75f /* load factor */, false) {
            public boolean removeEldestEntry(Map.Entry<Long, Map<String, Double>> eldest) {
                return size() > windowSize;
            }
        };

        this.fieldTemplate = fieldTemplate;
        this.timesteps = Collections.synchronizedMap(this.timesteps);
        this.featureDictionary = new ConcurrentSkipListMap<String, Integer>();

    }

    public int getNumOfPrincipalComponents() {
        return this.getNumPrincipalComponents();
    }

    /**
     * Must be called before any other functions. Declares and sets up internal data structures.
     *
     * @param numSamples Number of samples that will be processed.
     * @param features   Number of elements in each sample.
     */
    public void constructDataMatrixForPca(int numSamples, int features) {
        this.setMean(new double[features]);
        this.getDataMatrix().reshape(numSamples, features, false);
        this.sampleIndex = 0;
        this.setNumPrincipalComponents(-1);
    }

    /**
     * Adds a new sample of the raw data to internal data structure for later processing.  All the samples
     * must be added before computeBasis is called.
     *
     * @param trainingVector Sample from original raw data.
     */
    public void addSample(double[] trainingVector) {
        if (getDataMatrix().getNumCols() != trainingVector.length) {
            throw new IllegalArgumentException("Unexpected sample size");
        }
        if (sampleIndex >= getDataMatrix().getNumRows()) {
            throw new IllegalArgumentException("Too many samples");
        }

        for (int i = 0; i < trainingVector.length; i++) {
            getDataMatrix().set(sampleIndex, i, trainingVector[i]);
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
        Logger.getAnonymousLogger().log(Level.INFO, "Compute basis to get principal components.");

        validateData(numComponents);
        setNumPrincipalComponents(numComponents);
        computeNormalizedMean();

        // Compute SVD and save time by not computing U
        SingularValueDecomposition<DenseMatrix64F> svd =
                DecompositionFactory.svd(getDataMatrix().numRows, getDataMatrix().numCols, false, true, false);
        if (!svd.decompose(getDataMatrix())) {
            throw new IllegalStateException("SVD failure. Can't compute principal components!");
        }

        setPrincipalComponentSubspace(svd.getV(null, true));
        final DenseMatrix64F singularDiagonalMatrix = svd.getW(null);

        // Singular values are in an arbitrary order initially. We ask for principal components subspace to be transposed.
        SingularOps.descendingOrder(null, false, singularDiagonalMatrix, getPrincipalComponentSubspace(), true);

        // strip off unneeded components and find the basis
        getPrincipalComponentSubspace().reshape(numComponents, getMean().length, true);
    }

    public void validateData(int numComponents) {
        if (numComponents > getDataMatrix().getNumCols()) {
            throw new IllegalArgumentException("More components requested that the data's length.");
        }
        if (sampleIndex != getDataMatrix().getNumRows()) {
            throw new IllegalArgumentException("Not all the data has been added");
        }
        if (numComponents > sampleIndex) {
            throw new IllegalArgumentException(MessageFormat.format("More data needed to compute the desired number of components (sampleIndex={0}) (numComponents = {1}) ", sampleIndex, numComponents));
        }
    }

    protected void computeNormalizedMean() {
        // compute mean
        for (int i = 0; i < getDataMatrix().getNumRows(); i++) {
            for (int j = 0; j < getMean().length; j++) {
                getMean()[j] += getDataMatrix().get(i, j);
            }
        }
        for (int j = 0; j < getMean().length; j++) {
            getMean()[j] /= getDataMatrix().getNumRows();
        }

        // subtract the mean from the original data; and get zero mean data
        for (int i = 0; i < getDataMatrix().getNumRows(); i++) {
            for (int j = 0; j < getMean().length; j++) {
                getDataMatrix().set(i, j, getDataMatrix().get(i, j) - getMean()[j]);
            }
        }
    }

    /**
     * Returns all the principal components of data matrix A
     *
     * @return pca
     */
    public synchronized double[][] getPrincipalComponents() {
        int component = 0;
        final int numPrincipalComponents = this.getNumOfPrincipalComponents();
        // every MLStormSpout sets the runtime feature count.
        final int numFeatures = this.fieldTemplate.getRuntimeFeatureCount();
        final double[][] eigenRowMajor = new double[numFeatures][numPrincipalComponents];

        while (component < numPrincipalComponents) {
            final double[] basisColumnVector = this.getBasisVector(component);
            for (int featureIndex = 0; featureIndex < basisColumnVector.length; featureIndex++) {
                eigenRowMajor[featureIndex][component] = basisColumnVector[featureIndex];
            }
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
        if (which < 0 || which >= getNumPrincipalComponents()) {
            throw new IllegalArgumentException("Invalid component");
        }

        DenseMatrix64F v = new DenseMatrix64F(1, getDataMatrix().numCols);
        CommonOps.extract(getPrincipalComponentSubspace(), which, which + 1, 0, getDataMatrix().numCols, v, 0, 0);

        return v.data;
    }

    /**
     * Converts a vector from sample space into eigen space.
     *
     * @param sampleData Sample space data.
     * @return Eigen space projection.
     */
    public double[] sampleToEigenSpace(double[] sampleData) {
        if (sampleData.length != getDataMatrix().getNumCols()) {
            throw new IllegalArgumentException("Unexpected sample length");
        }
        DenseMatrix64F mean = DenseMatrix64F.wrap(getDataMatrix().getNumCols(), 1, this.getMean());

        DenseMatrix64F s = new DenseMatrix64F(getDataMatrix().getNumCols(), 1, true, sampleData);
        DenseMatrix64F r = new DenseMatrix64F(getNumPrincipalComponents(), 1);

        CommonOps.sub(s, mean, s);

        CommonOps.mult(getPrincipalComponentSubspace(), s, r);

        return r.data;
    }

    /**
     * Converts a vector from eigen space into sample space.
     *
     * @param eigenData Eigen space data.
     * @return Sample space projection.
     */
    public double[] eigenToSampleSpace(double[] eigenData) {
        if (eigenData.length != getNumPrincipalComponents()) {
            throw new IllegalArgumentException("Unexpected sample length");
        }

        DenseMatrix64F s = new DenseMatrix64F(getDataMatrix().getNumCols(), 1);
        DenseMatrix64F r = DenseMatrix64F.wrap(getNumPrincipalComponents(), 1, eigenData);

        CommonOps.multTransA(getPrincipalComponentSubspace(), r, s);

        DenseMatrix64F mean = DenseMatrix64F.wrap(getDataMatrix().getNumCols(), 1, this.getMean());
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
        if (sample.length != getDataMatrix().numCols) {
            throw new IllegalArgumentException("Expected input vector to be in sample space");
        }

        DenseMatrix64F dots = new DenseMatrix64F(getNumPrincipalComponents(), 1);
        DenseMatrix64F s = DenseMatrix64F.wrap(getDataMatrix().numCols, 1, sample);

        CommonOps.mult(getPrincipalComponentSubspace(), s, dots);
        return NormOps.normF(dots);
    }


    /**
     * Returns the latest map of sensor data in the window
     *
     * @return map of sensors and data
     */
    public Map<String, Double> getCurrentSamples() {
        return getCurrentSensorsAndReset(false);
    }


    /**
     * Internal
     *
     * @param reset
     * @return
     */
    protected synchronized Map<String, Double> getCurrentSensorsAndReset(final boolean reset) {
        final Map<String, Double> oldSensors = currentSamples;
        if (reset) {
            currentSamples = new ConcurrentSkipListMap<String, Double>();
        }
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
     * Returns the dictionary that maps sample ids to names
     *
     * @return Map
     */
    public Map<Integer, String> getReverseLookupFeatureDictionary() {
        return reverseLookupFeatureDictionary;
    }

    public DenseMatrix64F getPrincipalComponentSubspace() {
        return principalComponentSubspace;
    }

    protected void setPrincipalComponentSubspace(DenseMatrix64F principalComponentSubspace) {
        this.principalComponentSubspace = principalComponentSubspace;
    }

    public double[] getMean() {
        return mean;
    }

    public void setMean(double[] mean) {
        this.mean = mean;
    }

    /**
     * Rows of feature vectors
     * Columns are features
     *
     * @return data matrix
     */
    public DenseMatrix64F getDataMatrix() {
        return dataMatrix;
    }

    public void setDataMatrix(DenseMatrix64F dataMatrix) {
        this.dataMatrix = dataMatrix;
    }

    public int getNumPrincipalComponents() {
        return numPrincipalComponents;
    }

    public void setNumPrincipalComponents(int numPrincipalComponents) {
        this.numPrincipalComponents = numPrincipalComponents;
    }
}
