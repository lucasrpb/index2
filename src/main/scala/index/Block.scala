package index

trait Block[T, K, V] {

  val id: T

  def isFull(): Boolean
  def isEmpty(): Boolean
  def hasMinimumKeys(): Boolean
  def hasEnoughKeys(): Boolean
  def max: Option[K]

}
