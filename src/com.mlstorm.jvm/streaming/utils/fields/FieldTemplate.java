package utils.fields;
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

/**
 * A Field template is a key interface in MlStorm to define the Storm stream fields and the feature vectors.
 * It helps to consistently name the fields across topologies and streams.
 * <p/>
 * Every tuple in the top level Storm stream must consist of a key and a feature vector.
 * The merged streams can have arbitrary names, but the standard names are constants in this interface.
 */
public interface FieldTemplate {

    String[] getFields();

    String getKeyField();

    String getFeatureVectorField();

    int getNumFeatures();

    void setNumFeatures(int numFeatures);

    public interface FieldConstants {
        String ARGS = "args";
        String PARTITION = "partition";
        String RESULT = "result";

        public interface PCA{
            String PCA = "PCA";
            String PCA_DRPC = "PCA-DRPC";
            String PCA_COMPONENTS = "components";
            String PCA_EIGEN = "eigen";
        }

        public interface CLASSIFICATION {
            String LABEL = "label";
            String ACTUAL_LABEL = "actualLabel";
        }

        public interface CONSENSUS {
            String CONSENSUS_DRPC = "ClustererEnsemble-DRPC";
        }

    }
}
