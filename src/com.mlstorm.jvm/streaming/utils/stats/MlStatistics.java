package utils.stats;

/* license text */

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
