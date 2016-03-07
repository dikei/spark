package pt.tecnico.spark.util

import org.apache.spark.Logging
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.io.ICsvBeanWriter

import scala.collection.mutable

/**
  * Listener to calculate the stage runtime
  */
class StageRuntimeReportListener(csvWriter: ICsvBeanWriter, headers: Array[String]) extends SparkListener with Logging{

  private val taskInfoMetrics = mutable.HashMap[Int, mutable.Buffer[(TaskInfo, TaskMetrics)]]()


  /**
    * Called when a stage is submitted
    */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    taskInfoMetrics += stageSubmitted.stageInfo.stageId -> mutable.Buffer[(TaskInfo, TaskMetrics)]()
  }

  /**
    * Called when a stage completes successfully or fails, with information on the completed stage.
    */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val info = stageCompleted.stageInfo

    if (info.failureReason.isDefined) {
      // Skip on failure
      return
    }

    logInfo("Stage completed: " + info)
    logInfo("Number of tasks: " + info.numTasks)

    val runtime = info.completionTime.get - info.submissionTime.get
    log.info("Stage runtime: {} ms", runtime)

    var totalDuration = 0L
    var min = Long.MaxValue
    var max = 0L
    taskInfoMetrics.get(info.stageId).get.foreach { case (taskInfo, taskMetric) =>
      totalDuration += taskInfo.duration
      if (taskInfo.duration < min) {
        min = taskInfo.duration
      }
      if (taskInfo.duration > max) {
        max = taskInfo.duration
      }
    }

    val mean = totalDuration / info.numTasks
    val variance = taskInfoMetrics.get(info.stageId).get.map { case (taskInfo, taskMetric) =>
      val tmp = taskInfo.duration - mean
      tmp * tmp
    }.sum / info.numTasks

    log.info("Total task time: {} ms", totalDuration)
    log.info("Average task runtime: {} ms", mean)
    log.info("Fastest task: {} ms", min)
    log.info("Slowest task: {} ms", max)
    log.info("Standard deviation: {} ms", Math.sqrt(variance))

    val taskRuntimeStats = new TaskRuntimeStatistic
    taskRuntimeStats.setStageId(info.stageId)
    taskRuntimeStats.setAverage(mean)
    taskRuntimeStats.setFastest(min)
    taskRuntimeStats.setSlowest(max)
    taskRuntimeStats.setStandardDeviation(Math.sqrt(variance).toLong)

    val processor : Array[CellProcessor] = Array (
      new org.supercsv.cellprocessor.constraint.NotNull(),
      new org.supercsv.cellprocessor.constraint.NotNull(),
      new org.supercsv.cellprocessor.constraint.NotNull(),
      new org.supercsv.cellprocessor.constraint.NotNull(),
      new org.supercsv.cellprocessor.constraint.NotNull()
    )

    csvWriter.write(taskRuntimeStats, headers, processor)

    // Clear out the buffer to save memory
    taskInfoMetrics.remove(info.stageId)
  }

  /**
    * Save each task info and metrics
    */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    for (buffer <- taskInfoMetrics.get(taskEnd.stageId)) {
      if (taskEnd.taskInfo != null && taskEnd.taskMetrics != null) {
        buffer += ((taskEnd.taskInfo, taskEnd.taskMetrics))
      }
    }
  }
}
