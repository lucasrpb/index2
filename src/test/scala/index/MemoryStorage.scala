package index

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.reflect.ClassTag

class MemoryStorage[T: ClassTag, K: ClassTag, V: ClassTag]() extends Storage [T, K, V]{

  val BLOCKS = new TrieMap[T, Block[T, K, V]]()

  override def get(id: T): Future[Option[Block[T, K, V]]] = {
    Future.successful(BLOCKS.get(id))
  }

  override def save(blocks: TrieMap[T, Block[T, K, V]]): Future[Boolean] = {
    blocks.foreach{case (id, b) => BLOCKS.put(id, b)}
    Future.successful(true)
  }
}
