package utils.stats;

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

public class MlStatistics {
    private long commitLag;
    private long lastUpdate;
    private long avgAccuracy;
    private long totalCommits;
    private long trainingDuration;

    public long getTrainingDuration() {
        return trainingDuration;
    }

    public void setTrainingDuration(long trainingDuration) {
        this.trainingDuration = trainingDuration;
    }

    public long getCommitLag() {
        return commitLag;
    }

    public void setCommitLag(long commitLag) {
        this.commitLag = commitLag;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public long getTotalCommits() {
        return totalCommits;
    }

    public void setTotalCommits(long totalCommits) {
        this.totalCommits = totalCommits;
    }

    public long getAvgAccuracy() {
        return avgAccuracy;
    }

    public void setAvgAccuracy(long avgAccuracy) {
        this.avgAccuracy = avgAccuracy;
    }
}
