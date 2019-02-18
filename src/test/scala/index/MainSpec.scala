package index

import java.util.UUID
import java.util.concurrent.{Executors, ThreadLocalRandom}
import java.util.concurrent.atomic.AtomicReference

import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContext

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class MainSpec extends FlatSpec {

  implicit val ord = new Ordering[Int] {
    override def compare(x: Int, y: Int): Int =  x - y
  }

  val MAX_VALUE = 1000//Int.MaxValue

  def test(): Unit = {

    //implicit val ec = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

    val rand = ThreadLocalRandom.current()

    val DATA_ORDER = 4//rand.nextInt(4, 10)
    val META_ORDER = 4//rand.nextInt(4, 10)

    val DATA_MIN = DATA_ORDER - 1
    val DATA_MAX = DATA_ORDER*2 - 1

    val META_MIN = META_ORDER - 1
    val META_MAX = META_ORDER*2 - 1

    implicit val store = new MemoryStorage[String, Int, Int]()
    val root = new AtomicReference[IndexRef[String, Int, Int]](IndexRef(UUID.randomUUID.toString))

    def insert(): Future[(Boolean, Seq[(Int, Int)])] = {

      implicit val ctx = new MemoryContext[String, Int, Int](DATA_ORDER, META_ORDER, store)
      val old = root.get()
      val index = new Index[String, Int, Int](old)

      val n = rand.nextInt(1, 100)

      var list = Seq.empty[(Int, Int)]

      for(i<-0 until n){
        val k = rand.nextInt(0, MAX_VALUE)

        if(!list.exists(_._1 == k)){
          list = list :+ k -> k
        }
      }

      index.insert(list).flatMap { case (ok, _) =>

        if(!ok){
          Future.successful(false -> null)
        } else {
          store.save(ctx.blocks).map { ok =>
            (ok && root.compareAndSet(old, index.ref)) -> list
          }
        }
      }
    }

    val n = rand.nextInt(4, 10)

    var tasks = Seq.empty[Future[(Boolean, Seq[(Int, Int)])]]

    for(i<-0 until n){
      tasks = tasks :+ insert()
    }

    val result = Await.result(Future.sequence(tasks), 1 minute)
    val results_ok = result.filter(_._1).map(_._2)

    val data = results_ok.foldLeft(Seq.empty[(Int, Int)]){ case (p, n) => p ++ n}

    val dsorted = data.sorted
    val ref = root.get()
    val isorted = QueryAPI.inOrder(ref.root)

    println(s"order: ${DATA_ORDER}\n")
    println(s"dsorted: ${dsorted}\n")
    println(s"isroted: ${isorted}\n")
    println(s"n: ${result.length} succeed: ${results_ok.length} size: ${ref.size}\n")

    assert(dsorted.equals(isorted))
  }

  "index data " should "be equal to test data" in {

    val n = 1000

    for(i<-0 until n){
      test()
    }

  }

}
