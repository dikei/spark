package org.apache.spark.shuffle

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.{Logging, SparkConf, SparkEnv}
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, MemoryBlockObjectWriter, ShuffleBlockId}
import org.apache.spark.util.Utils
import java.io.OutputStream
import scala.collection.mutable

/**
  * Created by dikei on 4/20/16.
  */
class MemoryShuffleBlockResolver(conf: SparkConf) extends ShuffleBlockResolver with Logging {

  private lazy val blockManager = SparkEnv.get.blockManager
  private val shuffleToBlockId = new mutable.HashMap[Int, mutable.ArrayBuffer[ShuffleBlockId]]()
  private val bufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  /**
    * Retrieve the data for the specified block. If the data for that block is not available,
    * throws an unspecified exception.
    */
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    val bytes = blockManager.memoryStore.getBytes(blockId)
    bytes match {
      case Some(byteBuffer) => new NioManagedBuffer(byteBuffer)
      case _ =>
        logError(s"No block available for $blockId")
        throw new Exception(s"No block available for $blockId")
    }
  }

  override def stop(): Unit = {

  }

  def getWriters(
      shuffleId: Int,
      mapId: Int,
      numReducers: Int,
      serializer: Serializer,
      writeMetrics: ShuffleWriteMetrics): Array[MemoryBlockObjectWriter] = {

    val serializerInstance = serializer.newInstance()
    Array.tabulate[MemoryBlockObjectWriter](numReducers) { bucketId =>
      val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
      shuffleToBlockId.getOrElseUpdate(shuffleId, new mutable.ArrayBuffer[ShuffleBlockId]()) += blockId
      val compression: OutputStream => OutputStream = blockManager.wrapForCompression(blockId, _)
      val syncWrites = conf.getBoolean("spark.shuffle.sync", defaultValue = false)
      new MemoryBlockObjectWriter(serializerInstance, bufferSize, compression, syncWrites, writeMetrics, blockId)
    }
  }

  def removeShuffle(shuffleId: Int): Boolean = {
    for(blocks <- shuffleToBlockId.get(shuffleId)) {
      for (blockId <- blocks) {
        blockManager.memoryStore.remove(blockId)
      }
    }
    true
  }
}
