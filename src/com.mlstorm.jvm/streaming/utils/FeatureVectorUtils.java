package utils;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import weka.core.DenseInstance;

import java.io.*;

/**
 * Created by lbhat@DaMSl on 4/10/14.
 * <p/>
 * Copyright {2013} {Lakshmisha Bhat}
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
public class FeatureVectorUtils {
    public static weka.core.Instance buildInstance(double[] featureVector) {
        weka.core.Instance instance = new DenseInstance(featureVector.length);
        for (int i = 0; i < featureVector.length; i++) {
            instance.setValue(i, featureVector[i]);
        }
        return instance;
    }

    public static double[] deserializeToFeatureVector(String q) throws DecoderException, IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(Hex.decodeHex(q.toCharArray()));
        ObjectInputStream in = new ObjectInputStream(byteArrayInputStream);
        return (double[]) in.readObject();
    }

    public static String serializeFeatureVector(double[] both) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(both);
        return new String(Hex.encodeHex(byteArrayOutputStream.toByteArray()));
    }
}
