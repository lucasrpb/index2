package index

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.reflect.ClassTag

abstract class TxContext[T: ClassTag, K: ClassTag, V: ClassTag] {

  val parents: Parents[T, K, V] = new Parents[T, K, V]()
  val blocks = new TrieMap[T, Block[T, K, V]]()

  def createPartition(): Partition[T, K, V]
  def createMeta(): MetaBlock[T, K, V]

  def getBlock(id: T): Future[Option[Block[T, K, V]]]
  def getPartition(id: T): Future[Option[Partition[T, K, V]]]
  def getMeta(id: T): Future[Option[MetaBlock[T, K, V]]]

  def copy(block: Partition[T, K, V]): Partition[T, K, V]
  def copy(block: MetaBlock[T, K, V]): MetaBlock[T, K, V]

  def split(left: Partition[T, K, V]): Partition[T, K, V]
  def split(left: MetaBlock[T, K, V]): MetaBlock[T, K, V]

}
