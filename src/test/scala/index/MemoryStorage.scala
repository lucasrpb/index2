package index

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.reflect.ClassTag

class MemoryStorage[T: ClassTag, K: ClassTag, V: ClassTag]() extends Storage [T, K, V]{

  val blocks = new TrieMap[T, Block[T, K, V]]()

  override def get(id: T): Future[Option[Block[T, K, V]]] = {
    Future.successful(blocks.get(id))
  }

  override def save(blocks: TrieMap[T, Block[T, K, V]]): Future[Boolean] = {
    blocks.foreach{case (id, b) => this.blocks.put(id, b)}
    Future.successful(true)
  }

  override def put(block: Block[T, K, V]): Future[Boolean] = {
    blocks.put(block.id, block)
    Future.successful(true)
  }
}
