package kafka.log.es

import io.netty.buffer.Unpooled
import kafka.log._
import kafka.server.{FetchDataInfo, LogOffsetMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.utils.Time

import scala.jdk.CollectionConverters.IterableHasAsScala

class ElasticLogSegment(
                         val topicPartition: TopicPartition,
                         val dataStreamSegment: ElasticStreamSegment,
                         val timeStreamSegment: ElasticStreamSegment,
                         val txnStreamSegment: ElasticStreamSegment,
                         val nextOffset: Long,
                         baseOffset: Long,
                         indexIntervalBytes: Int,
                         rollJitterMs: Long,
                         time: Time) extends LogSegment(
  // TODO: override method, to mock access log, index, timeindex, txnindex from outside
  log = null,
  lazyOffsetIndex = null,
  lazyTimeIndex = null,
  txnIndex = null,
  baseOffset,
  indexIntervalBytes,
  rollJitterMs,
  time) {

  var fuzzySize: Integer = 0

  override def append(largestOffset: Long, largestTimestamp: Long, shallowOffsetOfMaxTimestamp: Long, records: MemoryRecords): Unit = {
    for (batch <- records.batches.asScala) {
      // TODO: ack should wait append complete
      fuzzySize+= batch.sizeInBytes()
      dataStreamSegment.append(new RecordBatchWrapper(batch))
      // TODO: timeindex insert
      // TODO: txnindex insert
    }
  }

  override def read(startOffset: Long, maxSize: Int, maxPosition: Long, minOneMessage: Boolean): FetchDataInfo = {
    val rst = dataStreamSegment.fetch(startOffset, maxSize).get()
    val offsetMetadata = LogOffsetMetadata(startOffset, baseOffset, 0)
    val compositeByteBuf = Unpooled.compositeBuffer();
    rst.recordBatchList().forEach(record => {
      compositeByteBuf.addComponent(true, Unpooled.wrappedBuffer(record.rawPayload()))
    })
    new FetchDataInfo(offsetMetadata, MemoryRecords.readableRecords(compositeByteBuf.nioBuffer()))
  }

  override def shouldRoll(rollParams: RollParams): Boolean = {
    //FIXME
    false
  }

  override def size: Int = {
    fuzzySize
  }
}
