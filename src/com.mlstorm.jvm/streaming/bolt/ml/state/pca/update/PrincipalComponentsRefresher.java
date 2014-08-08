package bolt.ml.state.pca.update;

import bolt.ml.state.pca.PrincipalComponentsBase;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

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

public class PrincipalComponentsRefresher implements StateUpdater<PrincipalComponentsBase> {
    @Override
    public void updateState(final PrincipalComponentsBase principalComponentsBase,
                            final List<TridentTuple> tuples,
                            final TridentCollector collector) {
        for (TridentTuple tuple : tuples) {
            // TODO figure out a way to perform state "merge"
            // TODO look at Candid covariance-free incremental principal component analysis (CCIPCA) paper.
            collector.emit(tuple);
        }
    }

    @Override
    public void prepare(final Map map, final TridentOperationContext tridentOperationContext) {
    }

    @Override
    public void cleanup() {

    }
}
