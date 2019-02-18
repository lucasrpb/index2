package index

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

trait Storage[T, K, V] {

  def get(id: T): Future[Option[Block[T, K, V]]]
  def put(block: Block[T, K, V]): Future[Boolean]
  def save(blocks: TrieMap[T, Block[T, K, V]]): Future[Boolean]

}
