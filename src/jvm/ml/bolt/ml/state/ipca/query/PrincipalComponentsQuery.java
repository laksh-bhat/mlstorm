package bolt.ml.state.ipca.query;

import backtype.storm.tuple.Values;
import bolt.ml.state.ipca.PrincipalComponentsBase;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.QueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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


public final class PrincipalComponentsQuery implements QueryFunction<PrincipalComponentsBase, Object> {
    @Override
    public List<Object> batchRetrieve(final PrincipalComponentsBase principalComponentsBase,
                                          final List<TridentTuple> queryTuples) {

        final List<Object> components = new ArrayList<Object>();
        for (TridentTuple ignored : queryTuples) components.add(principalComponentsBase.getPrincipalComponents());
        return components;
    }

    @Override
    public void execute(final TridentTuple queryTuple,
                        final Object component,
                        final TridentCollector tridentCollector) {
        tridentCollector.emit(new Values(component));
    }

    @Override
    public void prepare(final Map map, final TridentOperationContext tridentOperationContext) {}

    @Override
    public void cleanup() {}
}
