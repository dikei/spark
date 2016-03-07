package pt.tecnico.spark.util;

/**
 * POJO to save task runtime
 */
public class TaskRuntimeStatistic {
    private Integer stageId;
    private Long average;
    private Long fastest;
    private Long slowest;
    private Long standardDeviation;

    public Integer getStageId() {
        return stageId;
    }

    public void setStageId(Integer stageId) {
        this.stageId = stageId;
    }

    public Long getAverage() {
        return average;
    }

    public void setAverage(Long average) {
        this.average = average;
    }

    public Long getFastest() {
        return fastest;
    }

    public void setFastest(Long fastest) {
        this.fastest = fastest;
    }

    public Long getSlowest() {
        return slowest;
    }

    public void setSlowest(Long slowest) {
        this.slowest = slowest;
    }

    public Long getStandardDeviation() {
        return standardDeviation;
    }

    public void setStandardDeviation(Long standardDeviation) {
        this.standardDeviation = standardDeviation;
    }
}
