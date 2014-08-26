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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Implements Incremental updates to principal components state
 * todo
 */
public class IncrementalStormPca extends PrincipalComponentsBase {

    public IncrementalStormPca(int elementsInSample, int numPrincipalComponents, int localPartition, int numPartitions) throws Exception {
        super(elementsInSample, numPrincipalComponents, localPartition, numPartitions);
    }

    /**
     * Simply adds samples in the window to the data matrix by making sure 0's are added when a particular sensor isn't seen
     *
     * @param sensorNames
     * @param numColumns
     */
    private void addSamplesInWindowToMatrix(final Set<String> sensorNames, final int numColumns) {
        for (String sensorName : sensorNames) {
            int columnIndex = 0;
            double[] row = new double[numColumns];
            Iterator<Map<String, Double>> valuesIterator = windowTimesteps.values().iterator();
            while (valuesIterator.hasNext() && columnIndex < numColumns) {
                final Map<String, Double> timeStep = valuesIterator.next();
                row[columnIndex++] = timeStep.containsKey(sensorName) ? timeStep.get(sensorName) : 0.0;
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
            initializePCSubspace();
        }
    }

    public void initializePCSubspace() {
        final Set<String> sensorNames = this.sensorDictionary.keySet();
        final int numRows = this.sensorDictionary.size();
        final int numColumns = this.windowSize;

        super.constructDataMatrixForPca(numRows, numColumns);
        addSamplesInWindowToMatrix(sensorNames, numColumns);
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
}
