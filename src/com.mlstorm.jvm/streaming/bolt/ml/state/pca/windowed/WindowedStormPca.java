package bolt.ml.state.pca.windowed;

import bolt.ml.state.pca.PrincipalComponentsBase;

import java.text.MessageFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by lbhat@DaMSl on 1/9/14.
 * <p/>
 * Copyright {2013 - 2015} {Lakshmisha Bhat}
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

public class WindowedStormPca extends PrincipalComponentsBase {
    public WindowedStormPca(int elementsInSample, int numPrincipalComponents, int localPartition, int numPartitions) throws Exception {
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
            final double[] row = new double[numColumns];
            final Iterator<Map<String, Double>> valuesIterator = windowTimesteps.values().iterator();
            while (valuesIterator.hasNext() && columnIndex < numColumns) {
                final Map<String, Double> timeStep = valuesIterator.next();
                row[columnIndex++] = timeStep.containsKey(sensorName) ? timeStep.get(sensorName) : 0.0;
            }
            addSample(row);
        }
    }

    /**
     * Compute PCA and set all elements of data matrix to 0
     */
    private void computePrincipalComponentsAndResetDataMatrix() {
        computeBasis(numExpectedComponents);
        {   // Reset the data matrix and its index
            dataMatrix.zero();
            sampleIndex = 0;
        }
    }

    /**
     * Before we start a batch, storm tells us which "storm transaction" we are going to commit
     *
     * @param txid
     */
    @Override
    public void beginCommit(final Long txid) {
        Logger.getAnonymousLogger().log(Level.INFO, "PCA training starts now.");
    }

    /**
     * Nothing fancy. We push this sensor reading to the time-series window
     *
     * @param txId
     */
    @Override
    public synchronized void commit(final Long txId) {
        Logger.getAnonymousLogger().log(Level.INFO, MessageFormat.format("Commit called for transaction {0}", txId));

        final Set<String> sensorNames = this.sensorDictionary.keySet();
        final int numRows = this.sensorDictionary.size();
        final int numColumns = this.windowSize;

        Logger.getAnonymousLogger().log(Level.INFO, MessageFormat.format("matrix has {0} rows and {1} columns", numRows, numColumns));

        if (currentSensors.size() > numRows / 2 + 10 /*we expect 50% + 10 success rate*/) {
            windowTimesteps.put(txId, getCurrentSensorsAndReset(true));
        }
        if (windowTimesteps.size() < windowSize) {
            return;
        }

        constructDataMatrixForPca(numRows, numColumns);
        addSamplesInWindowToMatrix(sensorNames, numColumns);
        if (numRows > 0 && numColumns > 0) {
            computePrincipalComponentsAndResetDataMatrix();
        }
    }
}
