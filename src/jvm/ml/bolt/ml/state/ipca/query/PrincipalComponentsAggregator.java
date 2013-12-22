package bolt.ml.state.ipca.query;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

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


public class PrincipalComponentsAggregator implements CombinerAggregator<Double[][]> {
    @Override
    public Double[][] init (final TridentTuple components) {
        if (components.getValueByField("components") != null){
            return  (Double[][]) components.getValueByField("components");
        }
        else return null;
    }

    @Override
    public Double[][] combine (final Double[][] partition1, final Double[][] partition2) {
        if (partition1 == null) return partition2;
        else if (partition2 == null) return partition1;

        Double[][] combined = new Double[partition1.length + partition2.length][partition1[0].length];
        int i = 0;
        for (; i < partition1.length; i++)
            System.arraycopy(partition1, i, combined, i, partition1[i].length);

        for (int j = 0; j < partition2.length && i < combined.length; i++, j++)
            System.arraycopy(partition2, j, combined, i, partition2[j].length);

        return combined;
    }

    @Override
    public Double[][] zero () {
        return null;
    }
}
