package org.apache.spark.shuffle.hash

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle._
import org.apache.spark.storage.{MemoryBlockObjectWriter, ShuffleBlockId, StorageLevel}
import org.apache.spark.{Logging, SparkEnv, TaskContext}

/**
  * Created by dikei on 4/20/16.
  */
class MemoryShuffleWriter[K, V](
     handle: BaseShuffleHandle[K, V, _],
     mapId: Int,
     context: TaskContext,
     shuffleBlockResolver: MemoryShuffleBlockResolver) extends ShuffleWriter[K, V] with Logging{

  private val blockManager = SparkEnv.get.blockManager
  private val dep = handle.dependency
  private val serializer = Serializer.getSerializer(dep.serializer.orNull)
  private val writeMetrics = new ShuffleWriteMetrics()
  context.taskMetrics().shuffleWriteMetrics = Some(writeMetrics)

  private val writers: Array[MemoryBlockObjectWriter] =
    shuffleBlockResolver.getWriters(handle.shuffleId, mapId, dep.partitioner.numPartitions, serializer, writeMetrics)

  private var stopping = false

  /** Write a sequence of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val iter = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        dep.aggregator.get.combineValuesByKey(records, context)
      } else {
        records
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      records
    }

    for (elem <- iter) {
      val bucketId = dep.partitioner.getPartition(elem._1)
      writers(bucketId).write(elem._1, elem._2)
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      return None
    }
    stopping = true
    if (success) {
      try {
        Some(commit())
      } catch {
        case e: Exception =>
          cleanup()
          throw e
      }
    } else {
      cleanup()
      None
    }
  }

  /**
    * Save the block to memory store for retrieval
    */
  private def commit(): MapStatus = {
    val sizes = writers.zipWithIndex.map { case (writer, bucketId) =>
      if (writer.isOpen) {
        val blockId = new ShuffleBlockId(handle.shuffleId, mapId, bucketId)
        writer.close()
        blockManager.memoryStore.putBytes(blockId, writer.getByteBuffer(), StorageLevel.MEMORY_ONLY).size
      }
      else 0
    }

    MapStatus(blockManager.shuffleServerId, sizes)
  }

  /**
    * Cleanup
    */
  private def cleanup(): Unit = {
    writers.zipWithIndex.foreach { case(_, bucketId) =>
      val blockId = new ShuffleBlockId(handle.shuffleId, mapId, bucketId)
      blockManager.memoryStore.remove(blockId)
    }
  }
}
