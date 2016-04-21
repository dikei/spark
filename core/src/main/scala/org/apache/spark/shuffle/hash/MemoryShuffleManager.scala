package org.apache.spark.shuffle.hash

import org.apache.spark.{Logging, ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle._
import scala.collection.mutable

/**
  * Shuffle manager that keep all shuffle block in-memory
  */
class MemoryShuffleManager(conf: SparkConf)  extends ShuffleManager with Logging {

  private val memoryShuffleBlockResolver = new MemoryShuffleBlockResolver(conf)

  /**
    * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
    */
  override def registerShuffle[K, V, C](shuffleId: Int, numMaps: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    log.info("New shuffle {} with {} maps", shuffleId, numMaps)
    new BaseShuffleHandle[K, V, C](shuffleId, numMaps, dependency)
  }

  /**
    * Return a resolver capable of retrieving shuffle block data based on block coordinates.
    */
  override def shuffleBlockResolver: ShuffleBlockResolver = memoryShuffleBlockResolver

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    memoryShuffleBlockResolver.stop()
  }

  /**
    * Remove a shuffle's metadata from the ShuffleManager.
    *
    * @return true if the metadata removed successfully, otherwise false.
    */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    log.info("Removing shuffle {}", shuffleId)
    memoryShuffleBlockResolver.removeShuffle(shuffleId)
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V] = {
    new MemoryShuffleWriter[K, V](
      handle.asInstanceOf[BaseShuffleHandle[K, V, _]],
      mapId,
      context,
      memoryShuffleBlockResolver)
  }

  /**
    * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
    * Called on executors by reduce tasks.
    */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    new BlockStoreShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }
}
