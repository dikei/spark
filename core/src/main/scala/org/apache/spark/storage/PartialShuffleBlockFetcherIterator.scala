package org.apache.spark.storage

import java.io.InputStream

import org.apache.spark.MapOutputTracker._
import org.apache.spark.network.shuffle.ShuffleClient
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.{MapOutputTracker, TaskContext, Logging}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
  * A shuffle block fetcher that works even when the map output is not completed.
  * This is done by wrapping around the ShuffleBlockFetcher and refresh it as needed
  */
class PartialShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: ShuffleClient,
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker,
    shuffleId: Int,
    startPartition: Int,
    endPartition: Int,
    maxBytesInFlight: Long)
  extends Iterator[(BlockId, InputStream)] with Logging{

  private val readyBlocks = new mutable.HashSet[Int]()

  private var blockFetcherIter: ShuffleBlockFetcherIterator = null

  private var finished: Boolean = false

  // Initialize the block fetcher
  refreshBlockFetcher()

  override def hasNext: Boolean = {
    if (finished) {
      blockFetcherIter.hasNext
    } else {
      if (!blockFetcherIter.hasNext) {
        refreshBlockFetcher()
      }
      blockFetcherIter.hasNext
    }
  }

  override def next(): (BlockId, InputStream) = {
    blockFetcherIter.next()
  }

  /**
    * Refresh the block fetcher. Block until we have new block or there is nothing to read
    */
  private def refreshBlockFetcher(): Unit = {
    def statuses: Array[MapStatus] = {
      val out = mapOutputTracker.getStatuses(shuffleId)
      // Copy the array map status array
      out.synchronized {
        Array[MapStatus]() ++ out
      }
    }

    var statusWithIndex = statuses.zipWithIndex
    var newBlocksAvailable = statusWithIndex.exists { case (s, i) => s != null && !readyBlocks.contains(i) }
    while (!newBlocksAvailable) {
      // Wait until new block is available
      log.info("Waiting 1s for new block to be available")
      log.info("Status with index: {}", statusWithIndex)
      Thread.sleep(1000)
      statusWithIndex = statuses.zipWithIndex
      newBlocksAvailable = statusWithIndex.exists { case (s, i) => s != null && !readyBlocks.contains(i) }
    }

    val splitsByAddress = new mutable.HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
    statusWithIndex.foreach { case (status, mapId) =>
      if (status != null && !readyBlocks.contains(mapId)) {
        for (part <- startPartition until endPartition) {
          splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
            ((ShuffleBlockId(shuffleId, mapId, part), status.getSizeForBlock(part)))
        }
        readyBlocks += mapId
      }
    }

    log.info("Ready block: {}", readyBlocks)

    if (readyBlocks.size == statuses.length) {
      finished = true
    }

    blockFetcherIter = new ShuffleBlockFetcherIterator(
      context,
      shuffleClient,
      blockManager,
      splitsByAddress.toSeq,
      maxBytesInFlight)
  }
}
