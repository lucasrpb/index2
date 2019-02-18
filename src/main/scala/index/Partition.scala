package index

trait Partition[T, K, V] extends Block[T, K, V]{

  def insert(data: Seq[(K, V)]): (Boolean, Int)
  def remove(keys: Seq[K]): (Boolean, Int)
  def update(data: Seq[(K, V)]): (Boolean, Int)
  def inOrder(): Seq[(K, V)]

}
