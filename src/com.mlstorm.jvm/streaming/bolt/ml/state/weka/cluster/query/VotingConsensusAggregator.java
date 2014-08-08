package bolt.ml.state.weka.cluster.query;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;
import utils.fields.FieldTemplate;

import java.util.Map;
import java.util.TreeMap;

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

public class VotingConsensusAggregator implements ReducerAggregator<Map<Integer, Integer>> {
    @Override
    public Map<Integer, Integer> init() {
        // A map sorted by votes
        return new TreeMap<Integer, Integer>();
    }

    @Override
    public Map<Integer, Integer> reduce(Map<Integer, Integer> reducedResult, TridentTuple tuple) {
        //noinspection unchecked
        Map.Entry<Integer, double[]> clusterResult = (Map.Entry<Integer, double[]>) tuple.getValueByField(FieldTemplate.FieldConstants.RESULT);
        int partition = tuple.getIntegerByField(FieldTemplate.FieldConstants.PARTITION);

        if (reducedResult.containsValue(clusterResult.getKey())) {
            reducedResult.put(reducedResult.get(clusterResult.getKey()) + 1, clusterResult.getKey());
        } else {
            reducedResult.put(1, clusterResult.getKey());
        }

        return reducedResult;
    }
}
