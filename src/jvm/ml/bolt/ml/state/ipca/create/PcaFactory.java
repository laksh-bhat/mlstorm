package bolt.ml.state.ipca.create;


import backtype.storm.task.IMetricsContext;
import bolt.ml.state.ipca.PrincipalComponents;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.sql.SQLException;
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


public class PcaFactory implements StateFactory {
    final int sampleSize;
    PrincipalComponents pc = null;
    private final int numPrincipalComponents;

    public PcaFactory (int sampleSize, int numPrincipalComponents) {
        this.sampleSize = sampleSize;
        this.numPrincipalComponents = numPrincipalComponents;
    }

    @Override
    public synchronized State makeState (final Map conf,
                                         final IMetricsContext metrics,
                                         final int partitionIndex,
                                         final int numPartitions)
    {
        if (pc == null) try {
            pc = new PrincipalComponents(sampleSize, numPrincipalComponents, partitionIndex, numPartitions);
        } catch ( SQLException e ) {
            e.printStackTrace();
        }
        return pc;
    }
}
