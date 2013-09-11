package com.mikegagnon.decl

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

import cascading.flow.FlowDef
import com.twitter.scalding._

object LoadTaskSpec {

  import Dsl._

  def run(args: Args, loadTask: LoadTask)(implicit flowDef: FlowDef, mode: Mode)
      : TypedPipe[FeatureMap] =
    loadTask.load(TaskSet.initConfig(args, Set(loadTask), Nil))

  def testLoadTask[T <: Product](
      jobName: String,
      args: Map[String, List[String]],
      input: Iterable[T],
      inputSource: Source)
     (implicit manifest: Manifest[T]): JobTest = {
    val jobTest = JobTest("com.mikegagnon.decl." + jobName)
      .source(inputSource, input)

    args.foreach { case (key, value) =>
      jobTest.arg(key, value)
    }

    jobTest
  }

  val outputTsv = TypedTsv[FeatureMap]("output.tsv")
}

/**
 * Scalding jobs
 **************************************************************************************************/

// output all UserIds from input.tsv
class LoadTaskJob1(args: Args) extends Job(args) {
  import TaskSetSpec._
  val loadTask = LoadTask(TypedTsv[Long]("input.tsv") -> UserId) { userId: Long => userId }
  LoadTaskSpec.run(args, loadTask).write(LoadTaskSpec.outputTsv)
}

// output only even UserIds from input.tsv
class LoadTaskJob2(args: Args) extends Job(args) {
  import TaskSetSpec._
  val loadTask = LoadTask(TypedTsv[Long]("input.tsv") -> UserId) { userId: Long =>
    if (userId % 2 == 0) {
      Some(userId)
    } else {
      None
    }
  }
  LoadTaskSpec.run(args, loadTask).write(LoadTaskSpec.outputTsv)
}

class UserIdTweets(val userId: Long, val tweets: List[String])

// load UserId (required), Tweets (required), and SpamTweets (optional)
class LoadTaskJob3(args: Args) extends Job(args) {
  import TaskSetSpec._
  val loadTask =
    LoadTask((TypedTsv[UserIdTweets]("input.tsv"), Keywords) -> (UserId, Tweets, SpamTweets)) {
        args: (UserIdTweets, Set[String]) =>
      val (userId, tweets, keywords) = (args._1.userId, args._1.tweets, args._2)
      val spamTweets = tweets.toSet.filter{ t => keywords(t) }
      val spamTweetsOption = if (spamTweets.isEmpty) {
        None
      } else {
        Some(spamTweets)
      }

      (userId.toLong, tweets, spamTweetsOption)
    }
  LoadTaskSpec.run(args, loadTask).write(LoadTaskSpec.outputTsv)
}

/**
 * Tests
 **************************************************************************************************/
@RunWith(classOf[JUnitRunner])
class LoadTaskSpec extends FlatSpec with ShouldMatchers {

  import TaskSetSpec._
  import LoadTaskSpec._
  import Dsl._

  /**
   * Test cases with one input, one output
   ************************************************************************************************/

  "LoadTask.apply" should "work with zero config values and one required output feature" in {

    val input = List(
      Tuple1(1L),
      Tuple1(2L),
      Tuple1(3L),
      Tuple1(4L))

    val expectedOutput = List(
      FeatureMap(UserId -> 1L),
      FeatureMap(UserId -> 2L),
      FeatureMap(UserId -> 3L),
      FeatureMap(UserId -> 4L))

    testLoadTask(
        jobName = "LoadTaskJob1",
        args = Map(),
        input = input,
        TypedTsv[Long]("input.tsv"))
      .sink[FeatureMap](outputTsv) { buf =>
        buf.toList should equal (expectedOutput)
      }
      .run
      .finish
  }

  it should "work with zero config values and one optional output feature" in {

    val input = List(
      Tuple1(1L),
      Tuple1(2L),
      Tuple1(3L),
      Tuple1(4L))

    val expectedOutput = List(
      FeatureMap(),
      FeatureMap(UserId -> 2L),
      FeatureMap(),
      FeatureMap(UserId -> 4L))

    testLoadTask(
        jobName = "LoadTaskJob2",
        args = Map(),
        input = input,
        TypedTsv[Long]("input.tsv"))
      .sink[FeatureMap](outputTsv) { buf =>
        buf.toList should equal (expectedOutput)
      }
      .run
      .finish
  }

  /**
   * Test cases with a variety of inputs and outputs
   ************************************************************************************************/
  it should "work with one config value, two required outputs, and one optional output" in {

    val args = Map("Keywords" -> List("a", "b", "c"))

    val input = List(
      Tuple1(new UserIdTweets(1, List("x", "b", "y"))),
      Tuple1(new UserIdTweets(2, List())),
      Tuple1(new UserIdTweets(3, List("x", "y", "z"))),
      Tuple1(new UserIdTweets(4, List("b", "a", "c"))))

    val expectedOutput = List(
      FeatureMap(UserId -> 1L, Tweets -> List("x", "b", "y"), SpamTweets -> Set("b")),
      FeatureMap(UserId -> 2L, Tweets -> List[String]()),
      FeatureMap(UserId -> 3L, Tweets -> List("x", "y", "z")),
      FeatureMap(UserId -> 4L, Tweets -> List("b", "a", "c"), SpamTweets -> Set("a", "b", "c")))

    testLoadTask(
        jobName = "LoadTaskJob3",
        args = args,
        input = input,
        TypedTsv[UserIdTweets]("input.tsv"))
      .sink[FeatureMap](outputTsv) { buf =>
        buf.toList should equal (expectedOutput)
      }
      .run
      .finish
  }
}
