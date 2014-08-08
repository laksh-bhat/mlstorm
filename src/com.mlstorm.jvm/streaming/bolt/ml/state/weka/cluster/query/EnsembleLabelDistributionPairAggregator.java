package bolt.ml.state.weka.cluster.query;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;
import utils.fields.FieldTemplate;

import java.util.HashMap;
import java.util.Map;

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
public class EnsembleLabelDistributionPairAggregator implements ReducerAggregator<HashMap<Integer, Map.Entry<Integer, double[]>>> {
    @Override
    public HashMap<Integer, Map.Entry<Integer, double[]>> init() {
        // A map sorted by votes
        return new HashMap<Integer, Map.Entry<Integer, double[]>>();
    }

    @Override
    public HashMap<Integer, Map.Entry<Integer, double[]>> reduce(HashMap<Integer, Map.Entry<Integer, double[]>> reducedResult, TridentTuple tuple) {
        final int partition = tuple.getIntegerByField(FieldTemplate.FieldConstants.PARTITION);
        //noinspection unchecked
        final Map.Entry<Integer, double[]> clusterResult = (Map.Entry<Integer, double[]>) tuple.getValueByField(FieldTemplate.FieldConstants.RESULT);

        if (!reducedResult.containsKey(partition)) {
            reducedResult.put(partition, clusterResult);
        }

        return reducedResult;
    }
}
