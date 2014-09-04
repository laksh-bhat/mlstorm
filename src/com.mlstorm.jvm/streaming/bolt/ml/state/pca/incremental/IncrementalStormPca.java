/**
 * Created by lbhat@DaMSl on 1/9/14.
 * <p/>
 * Copyright {2013} {Lakshmisha Bhat}
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

package bolt.ml.state.pca.incremental;

import bolt.ml.state.pca.PrincipalComponentsBase;
import org.ejml.data.DenseMatrix64F;
import org.ejml.factory.DecompositionFactory;
import org.ejml.ops.CommonOps;
import org.ejml.simple.SimpleMatrix;
import utils.fields.FieldTemplate;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Implements Incremental updates to principal components state
 * todo
 */
public class IncrementalStormPca extends PrincipalComponentsBase {

    public IncrementalStormPca(int elementsInSample, int numPrincipalComponents, int localPartition, int numPartitions, FieldTemplate template) throws Exception {
        super(elementsInSample, numPrincipalComponents, localPartition, numPartitions, template);
    }

    /**
     * Simply adds samples in the window to the data matrix by making sure 0's are added when a particular feature isn't seen
     *
     * @param sampleNames keys identifying training samples
     * @param numColumnVectors total no. of columns we expect in a training feature vector
     */
    private void addSamplesInSlidingWindowToMatrix(final Set<String> sampleNames, final int numColumnVectors) {
        for (String sample : sampleNames) {
            int columnIndex = 0;
            double[] row = new double[numColumnVectors];
            Iterator<Map<String, Double>> timeStepsIterator = timesteps.values().iterator();
            while (timeStepsIterator.hasNext() && columnIndex < numColumnVectors) {
                final Map<String, Double> timeStep = timeStepsIterator.next();
                row[columnIndex++] = timeStep.containsKey(sample) ? timeStep.get(sample) : 0.0;
            }
            addSample(row);
        }
    }

    @Override
    public void beginCommit(Long txid) {
    }

    @Override
    public void commit(Long txid) {
        if(isEigenDecompositionPresent()){
            // do incremental processing of instances
        } else {
            // initialize eigen vectors and values by singular value decomposition
            // C ≈ (gamma)EpΛpE + + (1−gamma)yyT = AAT
            initializePrincipalComponentsSubspaceForIncrementalProcessing();
        }
    }

    public void initializePrincipalComponentsSubspaceForIncrementalProcessing() {
        final Set<String> sensorNames = this.featureDictionary.keySet();
        final int numRows = this.featureDictionary.size();
        final int numColumns = this.windowSize;

        super.constructDataMatrixForPca(numRows, numColumns);
        addSamplesInSlidingWindowToMatrix(sensorNames, numColumns);
        if (numRows > 0 && numColumns > 0) {
            computeBasis(numExpectedComponents);
        }
    }

    /**
     * Has PCA been initialized?
     * @return truth about PCA initialization
     */
    private boolean isEigenDecompositionPresent() {
        return getPrincipalComponentSubspace() != null;
    }

    public static void main(String[] args) {
        DenseMatrix64F y = new DenseMatrix64F(1, 2, true, new double[]{1.0, 2.0});
        DenseMatrix64F yT = CommonOps.transpose(y, null);
        DenseMatrix64F m = new DenseMatrix64F(2,2);
        CommonOps.mult(yT, y, m);

        SimpleMatrix m2 = new SimpleMatrix(2,2, true, new double[]{1,2,3,4});
        org.ejml.factory.SingularValueDecomposition<DenseMatrix64F> svd = DecompositionFactory.svd(2, 2, false, true, false);
        svd.decompose(m);
        m = svd.getV(null, true);

    }
}
