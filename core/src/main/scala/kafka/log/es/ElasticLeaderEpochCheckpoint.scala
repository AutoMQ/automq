package kafka.log.es

import kafka.server.checkpoints.LeaderEpochCheckpoint
import kafka.server.epoch.EpochEntry

import scala.jdk.CollectionConverters.{ListHasAsScala, SeqHasAsJava}

class ElasticLeaderEpochCheckpoint(val meta: ElasticLeaderEpochCheckpointMeta, val saveFunc: ElasticLeaderEpochCheckpointMeta => Unit) extends LeaderEpochCheckpoint{
    override def write(epochs: Iterable[EpochEntry]): Unit = this.synchronized {
        meta.setEntries(epochs.toList.asJava)
        saveFunc(meta)
    }

    override def read(): collection.Seq[EpochEntry] = this.synchronized {
        meta.entries().asScala.toSeq
    }
}
