/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.mikegagnon.decl

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

import cascading.flow.FlowDef

import com.twitter.algebird.Monoid
import com.twitter.scalding._

import scala.collection.mutable.Buffer

object TaskSetSpec {
  import Dsl._
  import TDsl._

  /**
   * Dummy features
   ************************************************************************************************/
  case object UserId extends Feature[Long]
  case object ScreenName extends Feature[String]
  case object Bio extends Feature[String]

  // lists of UserIds. followers.size <= MaxFollowings
  case object Followers extends Feature[List[Long]]
  case object Followees extends Feature[List[Long]]

  case object Tweets extends FeatureWithReducer[List[String]]

  // The subset of tweets that are considered spam
  case object SpamTweets extends FeatureWithReducer[Set[String]]

  // the length of followers and followees, respectively
  case object NumFollowers extends Feature[Int]
  case object NumFollowees extends Feature[Int]

  // 1.0 if the user has a screenName, bio, and at least one tweet
  // 0.0 otherwise
  case object IsBusiness extends Feature[Double]

  // 1.0 if any of the user's tweets exactly match any keyword for Keywords
  // 0.0 otherwise
  case object IsSpam extends Feature[Double]

  // 1.0 if the user has at least one tweet, follower, and followee
  // 0.0 otherwise
  case object IsEngaged extends Feature[Double]

  // This feature is erroneous because it is non-reducible yet there are duplicate values for the
  // feature
  case object ErroneousFeature extends Feature[Double]

  // Error case: CycleFeature1 depends on CycleFeature2 and vice versa
  case object CycleFeature1 extends Feature[Double]
  case object CycleFeature2 extends Feature[Double]

  // No tasks compute this feature, so requesting this feature will result in an exception
  case object OrphanFeature extends Feature[Double]

  /**
   * Config values
   ************************************************************************************************/
  case object MaxFollowings extends ConfigValue[Int] {
    override def init(args: Args) = args("MaxFollowings").toInt
  }

  case object Keywords extends ConfigValue[Set[String]] {
    override def init(args: Args) = args.list("Keywords").toSet
  }

  case object MaxTweetLen extends ConfigValue[Option[Int]] {
    override def init(args: Args) = args.optional("MaxTweetLen").map{ _.toInt }
  }

  /**
   * Load tasks
   ************************************************************************************************/
  case object LoadUserTable extends LoadTask {
    override val inputConfig = Set[ConfigValue[_]]()
    override val outputFeatures = Set[FeatureBase[_]](UserId, ScreenName, Bio)

    override def load(config: ConfigMap)(implicit flowDef: FlowDef, mode: Mode)
        : TypedPipe[FeatureMap] = {
      TypedTsv[(Long, String, String)]("LoadUserTable.tsv")
        .map { case (userId, sn, bio) =>
          FeatureMap(
            UserId -> userId,
            ScreenName -> sn,
            Bio -> bio)
        }
    }
  }

  case object LoadFollowings extends LoadTask {
    override val inputConfig = Set[ConfigValue[_]](MaxFollowings)
    override val outputFeatures = Set[FeatureBase[_]](UserId, Followees, Followers)

    override def load(config: ConfigMap)(implicit flowDef: FlowDef, mode: Mode)
        : TypedPipe[FeatureMap] = {

      val maxFollowings = config(MaxFollowings)

      TypedTsv[(Long, List[Long], List[Long])]("LoadFollowings.tsv")
        .map { case (userId, followees, followers) =>
          FeatureMap(
            UserId -> userId,
            Followees -> followees.take(maxFollowings),
            Followers -> followers.take(maxFollowings))
        }
    }
  }

  case object LoadTweets extends LoadTask {
    override val inputConfig = Set[ConfigValue[_]](MaxTweetLen)
    override val outputFeatures = Set[FeatureBase[_]](UserId, Tweets)

    override def load(config: ConfigMap)(implicit flowDef: FlowDef, mode: Mode)
        : TypedPipe[FeatureMap] = {

      val maxTweetLen = config(MaxTweetLen)

      TypedTsv[(Long, String)]("LoadTweets.tsv")
        .map { case (userId, tweet) =>

          val tweetTrimmed = maxTweetLen match {
            case None => tweet
            case Some(len) => tweet.take(len)
          }

          FeatureMap(
            UserId -> userId,
            Tweets -> List(tweetTrimmed))
        }
    }
  }

  case object LoadErroneousFeature extends LoadTask {
    override val inputConfig = Set[ConfigValue[_]]()
    override val outputFeatures = Set[FeatureBase[_]](UserId, ErroneousFeature)

    override def load(config: ConfigMap)(implicit flowDef: FlowDef, mode: Mode)
        : TypedPipe[FeatureMap] = {
      TypedTsv[(Long, Double)]("LoadErroneousFeature.tsv")
        .map { case (userId, num) =>
          FeatureMap(
            UserId -> userId,
            ErroneousFeature -> num)
        }
    }
  }

  /**
   * Compute tasks
   ************************************************************************************************/

  case object ComputeFollowCounts extends ComputeTask {
    override val inputConfig = Set[ConfigValue[_]]()
    override val inputFeatures = Set[FeatureBase[_]](Followees, Followers)
    override val outputFeatures = Set[FeatureBase[_]](NumFollowers, NumFollowees)

    override def compute(features: FeatureMap, config: ConfigMap): FeatureMap = {

      val numFollowees = features.get(Followees)
        .map{ followees => FeatureMap(NumFollowees -> followees.size) }
        .getOrElse(FeatureMap())
      val numFollowers = features.get(Followers)
        .map{ followers => FeatureMap(NumFollowers -> followers.size) }
        .getOrElse(FeatureMap())
      Monoid.plus(numFollowees, numFollowers)
    }
  }

  case object ComputeBusiness extends ComputeTask {
    override val inputConfig = Set[ConfigValue[_]]()
    override val inputFeatures = Set[FeatureBase[_]](Bio, ScreenName, Tweets)
    override val outputFeatures = Set[FeatureBase[_]](IsBusiness)

    override def compute(features: FeatureMap, config: ConfigMap): FeatureMap = {
      (features.get(Bio), features.get(ScreenName), features.get(Tweets)) match {
        case (Some(bio), Some(sn), Some(head :: tail)) => FeatureMap(IsBusiness -> 1.0)
        case _ => FeatureMap(IsBusiness -> 0.0)
      }
    }
  }

  case object ComputeIsSpam extends ComputeTask {
    override val inputConfig = Set[ConfigValue[_]](Keywords)
    override val inputFeatures = Set[FeatureBase[_]](Tweets)
    override val outputFeatures = Set[FeatureBase[_]](IsSpam)

    override def compute(features: FeatureMap, config: ConfigMap): FeatureMap = {

      features.get(Tweets)
        .map{ tweets =>
          val matches = tweets.toSet & config(Keywords)
          if (matches.isEmpty) {
            FeatureMap(IsSpam -> 0.0)
          } else {
            FeatureMap(IsSpam -> 1.0)
          }
        }
        .getOrElse(FeatureMap(IsSpam -> 0.0))
    }
  }

  class ComputeSpamTweets(keyword: String) extends ComputeTask {
    override val inputConfig = Set[ConfigValue[_]]()
    override val inputFeatures = Set[FeatureBase[_]](Tweets)
    override val outputFeatures = Set[FeatureBase[_]](SpamTweets)

    override def compute(features: FeatureMap, config: ConfigMap): FeatureMap = {

      features.get(Tweets)
        .map{ tweets =>
          val spamTweets = tweets
            .toSet
            .filter(_ == keyword)
          FeatureMap(SpamTweets -> spamTweets)
        }
        .getOrElse(FeatureMap())
    }
  }

  // these two tasks compute the same feature, but do it differently
  case object ComputeSpamTweets1 extends ComputeSpamTweets("spam1")
  case object ComputeSpamTweets2 extends ComputeSpamTweets("spam2")

  case object ComputeEngaged extends ComputeTask {
    override val inputConfig = Set[ConfigValue[_]]()
    override val inputFeatures = Set[FeatureBase[_]](Tweets, NumFollowers, NumFollowees)
    override val outputFeatures = Set[FeatureBase[_]](IsEngaged)

    override def compute(features: FeatureMap, config: ConfigMap): FeatureMap = {

      (features.get(Tweets), features.get(NumFollowers), features.get(NumFollowees)) match {
        case (Some(head :: tail), Some(numFollowers), Some(numFollowees)) => {
          if (numFollowers > 0 && numFollowees > 0) {
            FeatureMap(IsEngaged -> 1.0)
          } else {
            FeatureMap(IsEngaged -> 0.0)
          }
        }
        case _ => FeatureMap(IsEngaged -> 0.0)
      }
    }
  }

  case object ComputeCycle1 extends ComputeTask {
    override val inputConfig = Set[ConfigValue[_]]()
    override val inputFeatures = Set[FeatureBase[_]](CycleFeature2)
    override val outputFeatures = Set[FeatureBase[_]](CycleFeature1)
    override def compute(features: FeatureMap, config: ConfigMap): FeatureMap = FeatureMap()
  }

  case object ComputeCycle2 extends ComputeTask {
    override val inputConfig = Set[ConfigValue[_]]()
    override val inputFeatures = Set[FeatureBase[_]](CycleFeature1)
    override val outputFeatures = Set[FeatureBase[_]](CycleFeature2)
    override def compute(features: FeatureMap, config: ConfigMap): FeatureMap = FeatureMap()
  }

  /**
   * TaskSet
   ************************************************************************************************/

  val dummyTasks = Set[Task](
    LoadUserTable,
    LoadFollowings,
    LoadTweets,
    LoadErroneousFeature,
    ComputeFollowCounts,
    ComputeBusiness,
    ComputeIsSpam,
    ComputeEngaged,
    ComputeSpamTweets1,
    ComputeSpamTweets2,
    ComputeCycle1,
    ComputeCycle2)

  object Users extends TaskSet[Long] {
    override val groupByKey = UserId
    override val tasks = dummyTasks
  }

  /**
   * Dummy data
   ************************************************************************************************/

  // userId, screenName, bio
  val userTableData: List[(Long, String, String)] = List(
    (1L, "foo", "bar"),
    (2L, "baz", "pickle"))

  // userId, followees, followers
  val followingsData: List[(Long, List[Long], List[Long])] = List(
    (1L, List(2L, 3L, 4L), List(4L, 5L, 6L)),
    (3L, List(1L, 2L), List(6L)))

  val tweetData: List[(Long, String)] = List(
    (1L, "spam1"),
    (1L, "spam2"),
    (1L, "c"),
    (2L, "spam2"),
    (2L, "pickle"),
    (3L, "bananas"))

  // This data is erroneous because user 1L has multiple records
  val erroneousFeatureData: List[(Long, Double)] = List(
    (1L, 1.0),
    (1L, 2.0),
    (2L, 3.0))

  /**
   * Helper functions
   ************************************************************************************************/

  def testJob[T: Manifest](jobName: String, args: Map[String, List[String]]): JobTest = {
    val jobTest = JobTest("com.mikegagnon.decl." + jobName)
      .source(TypedTsv[(Long, String, String)]("LoadUserTable.tsv"), userTableData)
      .source(TypedTsv[(Long, List[Long], List[Long])]("LoadFollowings.tsv"), followingsData)
      .source(TypedTsv[(Long, String)]("LoadTweets.tsv"), tweetData)
      .source(TypedTsv[(Long, Double)]("LoadErroneousFeature.tsv"), erroneousFeatureData)

    args.foreach { case (key, value) =>
      jobTest.arg(key, value)
    }

    jobTest

  }
}

/**
 * Scalding jobs
 **************************************************************************************************/

class ScreenNameJob(args: Args) extends Job(args) {
  import TaskSetSpec._
  Users.get(args, UserId, ScreenName)
    .write(TypedTsv[(Option[Long], Option[String])]("output.tsv"))
}

class TweetsJob(args: Args) extends Job(args) {
  import TaskSetSpec._
  Users.get(args, UserId, Tweets)
    .write(TypedTsv[(Option[Long], Option[List[String]])]("output.tsv"))
}

class FollowersScreenNameJob(args: Args) extends Job(args) {
  import TaskSetSpec._
  Users.get(args, UserId, Followers, ScreenName)
    .write(TypedTsv[(Option[Long], Option[List[Long]], Option[String])]("output.tsv"))
}

class ComputeNumFollowersJob(args: Args) extends Job(args) {
  import TaskSetSpec._
  Users.get(args, UserId, ScreenName, NumFollowers)
    .write(TypedTsv[(Option[Long], Option[String], Option[Int])]("output.tsv"))
}

class ComputeSpamTweetsJob(args: Args) extends Job(args) {
  import TaskSetSpec._
  Users.get(args, UserId, SpamTweets)
    .write(TypedTsv[(Option[Long], Option[Set[String]])]("output.tsv"))
}

class IsBusinessJob(args: Args) extends Job(args) {
  import TaskSetSpec._
  Users.get(args, UserId, IsBusiness)
    .write(TypedTsv[(Option[Long], Option[Double])]("output.tsv"))
}

class IsSpamJob(args: Args) extends Job(args) {
  import TaskSetSpec._
  Users.get(args, UserId, IsSpam)
    .write(TypedTsv[(Option[Long], Option[Double])]("output.tsv"))
}

class IsEngagedJob(args: Args) extends Job(args) {
  import TaskSetSpec._
  Users.get(args, UserId, IsEngaged)
    .write(TypedTsv[(Option[Long], Option[Double])]("output.tsv"))
}

class ErroneousFeatureJob(args: Args) extends Job(args) {
  import TaskSetSpec._
  Users.get(args, UserId, ErroneousFeature)
    .write(TypedTsv[(Option[Long], Option[Double])]("output.tsv"))
}

class ErroneousCycleJob(args: Args) extends Job(args) {
  import TaskSetSpec._
  Users.get(args, UserId, CycleFeature1)
    .write(TypedTsv[(Option[Long], Option[Double])]("output.tsv"))
}

class ErroneousOrphanJob(args: Args) extends Job(args) {
  import TaskSetSpec._
  Users.get(args, UserId, OrphanFeature)
    .write(TypedTsv[(Option[Long], Option[Double])]("output.tsv"))
}

/**
 * Tests
 **************************************************************************************************/
@RunWith(classOf[JUnitRunner])
class TaskSetSpec extends FlatSpec with ShouldMatchers {

  import Dsl._
  import TaskSetSpec._

  /**
   * TaskSet.initConfig
   ************************************************************************************************/
  "TaskSet.initConfig" should "initialize MaxTweetLen to None when args is empty" in {
    TaskSet.initConfig(
      Args(""),
      loadTasks=Set(LoadTweets),
      computeTasks=Nil) should equal (ConfigMap(MaxTweetLen -> None))
  }

  it should "initialize MaxTweetLen to correct value when args contains the value" in {
    TaskSet.initConfig(
      Args("--MaxTweetLen 140"),
      loadTasks=Set(LoadTweets),
      computeTasks=Nil) should equal (ConfigMap(MaxTweetLen -> Some(140)))
  }

  it should "initialize Keywords and MaxTweetLen to empty value when args is empty" in {
    TaskSet.initConfig(
      Args(""),
      loadTasks=Set(LoadTweets),
      computeTasks=List(ComputeIsSpam)) should equal(ConfigMap(
        MaxTweetLen -> None,
        Keywords -> Set[String]()))
  }

  it should "initialize Keywords to correct value when args contains the value" in {
    TaskSet.initConfig(
      Args("--Keywords a b c"),
      loadTasks=Set(LoadTweets),
      computeTasks=List(ComputeIsSpam)) should equal(ConfigMap(
        MaxTweetLen -> None,
        Keywords -> Set("a", "b", "c")))
  }

  it should "throw exception if MaxFollowings is needed but args is empty" in {
    evaluating {
      TaskSet.initConfig(
        Args(""),
        loadTasks=Set(LoadFollowings),
        computeTasks=Nil)
    } should produce [RuntimeException]
  }

  it should "initialize MaxFollowings to correct value when args contains the value" in {
    TaskSet.initConfig(
      Args("--MaxFollowings 7"),
      loadTasks=Set(LoadFollowings),
      computeTasks=Nil) should equal(ConfigMap(MaxFollowings -> 7))
  }

  /**
   * TaskSet.scheduleTasks
   ************************************************************************************************/
  "TaskSet.scheduleTasks" should "schedule a single load task if that is all that is needed" in {
    TaskSet.scheduleTasks(dummyTasks, Set(ScreenName)) should equal (
      Set(LoadUserTable),
      Nil)
  }

  it should "schedule a single load task if that is all that is needed for multiple features" in {
    TaskSet.scheduleTasks(dummyTasks, Set(ScreenName, Bio)) should equal (
      Set(LoadUserTable),
      Nil)
  }

  it should "schedule load and compute tasks if a single computed feature is needed" in {
    TaskSet.scheduleTasks(dummyTasks, Set(IsBusiness)) should equal (
      Set(LoadUserTable, LoadTweets),
      List(ComputeBusiness))
  }

  it should "schedule __ordered__ compute tasks if one computed feature depends on another" in {
    TaskSet.scheduleTasks(dummyTasks, Set(IsEngaged)) should equal (
      Set(LoadFollowings, LoadTweets),
      List(ComputeFollowCounts, ComputeEngaged))
  }

  it should "schedule many tasks if many features are needed" in {

    val result = TaskSet.scheduleTasks(dummyTasks, Set(IsEngaged, IsBusiness))

    val expectedLoadTasks = Set[LoadTask](LoadUserTable, LoadFollowings, LoadTweets)

    /**
     * computeFollowCounts, computeEngaged must be ordered relative to each other
     * computeBusiness can appear anywhere
     */
    val possibleResults = Set[(Set[LoadTask], List[ComputeTask])] (
      (expectedLoadTasks, List(ComputeBusiness, ComputeFollowCounts, ComputeEngaged)),
      (expectedLoadTasks, List(ComputeFollowCounts, ComputeBusiness, ComputeEngaged)),
      (expectedLoadTasks, List(ComputeFollowCounts, ComputeEngaged, ComputeBusiness)))

    assert(possibleResults.contains(result))

  }

  /**
   * TaskSet.get
   ************************************************************************************************/

   "ScreenNameJob" should "fetch two features from a single source" in {

    type OutputTuple = (Option[Long], Option[String])

    val expectedOutput: Set[OutputTuple] = Set(
      (Some(1L), Some("foo")),
      (Some(2L), Some("baz")))

    testJob("ScreenNameJob", args = Map())
      .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
        buf.toSet should equal (expectedOutput)
      }
      .run
      .finish
  }

  "TweetsJob" should
    "reduce multiple tweet records into a single Tweets feature and use a configuration value" in {

    type OutputTuple = (Option[Long], Option[List[String]])

    val expectedOutput: Set[OutputTuple] = Set(
      (Some(1L), Some(List("s", "s", "c"))),
      (Some(2L), Some(List("s", "p"))),
      (Some(3L), Some(List("b"))))

    testJob(
        jobName ="TweetsJob",
        args = Map("MaxTweetLen" -> List("1")))
      .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
        buf.toSet should equal (expectedOutput)
      }
      .run
      .finish
  }

  "FollowersScreenNameJob" should "fetch and join three features from multiple sources" in {

    type OutputTuple = (Option[Long], Option[List[Long]], Option[String])

    val expectedOutput: Set[OutputTuple] = Set(
      (Some(1L), Some(List(4L, 5L)), Some("foo")),
      (Some(2L), None, Some("baz")),
      (Some(3L), Some(List(6L)), None))

    testJob(
        jobName = "FollowersScreenNameJob",
        args = Map("MaxFollowings" -> List("2")))
      .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
        buf.toSet should equal (expectedOutput)
      }
      .run
      .finish
  }

  "ComputeNumFollowersJob" should "compute a simple feature" in {

    type OutputTuple = (Option[Long], Option[String], Option[Int])

    val expectedOutput: Set[OutputTuple] = Set(
      (Some(1L), Some("foo"), Some(3)),
      (Some(2L), Some("baz"), None),
      (Some(3L), None, Some(1)))

    testJob(
        jobName = "ComputeNumFollowersJob",
        args = Map("MaxFollowings" -> List("10")))
      .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
        buf.toSet should equal (expectedOutput)
      }
      .run
      .finish
  }

  "ComputeSpamTweetsJob" should
    ("yield a single feature that is computed by two distinct compute tasks, " +
     "and reduced via the feature's semigroup") in {

    type OutputTuple = (Option[Long], Option[Set[String]])

    val expectedOutput: Set[OutputTuple] = Set(
      (Some(1L), Some(Set("spam1", "spam2"))),
      (Some(2L), Some(Set("spam2"))),
      (Some(3L), Some(Set())))

    testJob(jobName = "ComputeSpamTweetsJob", args = Map())
      .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
        buf.toSet should equal (expectedOutput)
      }
      .run
      .finish
  }

  "IsBusinessJob" should "compute a feature that is derived from multiple sources" in {

    type OutputTuple = (Option[Long], Option[Double])

    val expectedOutput: Set[OutputTuple] = Set(
      (Some(1L), Some(1.0)),
      (Some(2L), Some(1.0)),
      (Some(3L), Some(0.0)))

    testJob(jobName = "IsBusinessJob", args = Map())
      .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
        buf.toSet should equal (expectedOutput)
      }
      .run
      .finish
  }

  "IsEngagedJob" should
    "compute a feature that as function of other computed features and a loaded feature" in {

    type OutputTuple = (Option[Long], Option[Double])

    val expectedOutput: Set[OutputTuple] = Set(
      (Some(1L), Some(1.0)),
      (Some(2L), Some(0.0)),
      (Some(3L), Some(1.0)))

    testJob(
        jobName = "IsEngagedJob",
        args = Map("MaxFollowings" -> List("10")))
      .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
        buf.toSet should equal (expectedOutput)
      }
      .run
      .finish
  }

  /**
   * calls to TaskSet.get that raise an exception.
   * TODO: is there a way to silence the error messages caused by these test cases?
   ************************************************************************************************/

  "ErroneousFeatureJob" should
    "throw a FlowException caused by a DuplicateFeatureException exception" in {

    type OutputTuple = (Option[Long], Option[Double])

    evaluating {
      testJob(
          jobName = "ErroneousFeatureJob",
          args = Map())
        .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
          buf.toSet should equal (Set())
        }
        .run
        .finish
    } should produce [cascading.flow.FlowException]
  }

  "ErroneousCycleJob" should
    "throw a InvocationTargetException caused by a CycleException exception" in {

    type OutputTuple = (Option[Long], Option[Double])

    evaluating {
      testJob(
          jobName = "ErroneousCycleJob",
          args = Map())
        .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
          buf.toSet should equal (Set())
        }
        .run
        .finish
    } should produce [java.lang.reflect.InvocationTargetException]
  }

  "ComputeNumFollowersJob" should
    "throw a InvocationTargetExcept exception if MaxFollowings isn't specified" in {

    type OutputTuple = (Option[Long], Option[String], Option[Int])

    evaluating {
      testJob(
          jobName = "ComputeNumFollowersJob",
          args = Map())
        .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
          buf.toSet should equal (Set())
        }
        .run
        .finish
    } should produce [java.lang.reflect.InvocationTargetException]
  }

  "ErroneousOrphanJob" should
    "throw a InvocationTargetException caused by a NoLoadTasksException exception" in {

    type OutputTuple = (Option[Long], Option[Double])

    evaluating {
      testJob(
          jobName = "ErroneousOrphanJob",
          args = Map())
        .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
          buf.toSet should equal (Set())
        }
        .run
        .finish
    } should produce [java.lang.reflect.InvocationTargetException]
  }
}
