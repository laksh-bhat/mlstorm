package bolt.ml.state.pca.query;

import backtype.storm.tuple.Values;
import bolt.ml.state.pca.PrincipalComponentsBase;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.QueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
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


public final class PrincipalComponentsQuery implements QueryFunction<PrincipalComponentsBase, Object> {
    private int partition;
    private int numPartitions;

    @Override
    public List<Object> batchRetrieve(final PrincipalComponentsBase principalComponentsBase,
                                      final List<TridentTuple> queryTuples) {

        final List<Object> components = new ArrayList<Object>();
        for (TridentTuple ignored : queryTuples) {
            components.add(principalComponentsBase.getPrincipalComponents());
        }
        return components;
    }

    @Override
    public void execute(final TridentTuple queryTuple,
                        final Object component,
                        final TridentCollector tridentCollector) {
        tridentCollector.emit(new Values(component));
    }

    @Override
    public void prepare(final Map map, final TridentOperationContext tridentOperationContext) {
        this.partition = tridentOperationContext.getPartitionIndex() + 1;
        this.numPartitions = tridentOperationContext.numPartitions();
    }

    @Override
    public void cleanup() {
    }
}
