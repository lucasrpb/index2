package index

trait Partition[T, K, V] extends Block[T, K, V]{

  def insert(data: Seq[(K, V)]): (Boolean, Int)
  def remove(keys: Seq[K]): (Boolean, Int)
  def update(data: Seq[(K, V)]): (Boolean, Int)
  def inOrder(): Seq[(K, V)]

  def canBorrowTo(t: Partition[T, K, V]): Boolean
  def borrowRightTo(t: Partition[T, K, V]): Partition[T, K, V]
  def borrowLeftTo(t: Partition[T, K, V]): Partition[T, K, V]
  def merge(r: Partition[T, K, V]): Partition[T, K, V]

}
