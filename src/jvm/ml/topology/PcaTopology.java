package topology;

import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import spout.text.FileStreamingSpout;
import storm.trident.Stream;
import storm.trident.TridentTopology;

/**
 *
 * User: lbhat@DaMSl on 12/12/13.
 * Time: 9:53 PM

   Copyright {2013} {Lakshmisha Bhat <laksh85@gmail.com>}

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License
 */

public class PcaTopology {
    public static void main(String[] args) {
        String filename = args[0];
        String[] fields = {"sensorData"};
        int parallelism = args.length > 1? Integer.valueOf(args[1]) : 8;

        TridentTopology stormTopology = buildTopology(parallelism, filename, fields);
    }

    private static TridentTopology buildTopology (final int parallelism,
                                       final String filename,
                                       final String[] fields)
    {
        IRichSpout sensorSpout = new FileStreamingSpout(filename, fields);
        TridentTopology topology = new TridentTopology();
        Stream sensorStream = topology.newStream("sensor", sensorSpout);

        sensorStream.partitionBy(new Fields("sensor")).partitionPersist()
    }
}
