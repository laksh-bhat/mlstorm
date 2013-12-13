package topology;

import backtype.storm.topology.TopologyBuilder;
import spout.FileStreamingSpout;

/**
 * Created by lbhat@DaMSl on 12/12/13.

   Copyright {2013} {Lakshmisha Bhat}

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

public class PcaTopology {
    public static void main(String[] args) {
        String filename = args[0];
        String[] fields = {"sensorData"};

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sensor", new FileStreamingSpout(filename, fields), 10);
        builder.setStateSpout();


        builder.setBolt("exclaim1", new ExclamationBolt(), 3)
                .shuffleGrouping("words");
        builder.setBolt("exclaim2", new ExclamationBolt(), 2)
                .shuffleGrouping("exclaim1");
    }
}
