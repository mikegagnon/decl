package com.mikegagnon.decl

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

import com.twitter.scalding._

/**
 * Defines load and compute tasks that are semantically identical to the ones in TaskSetSpec.
 * The only difference is that these use the TaskBuilder shorthand syntax for task specification.
 *
 * The test cases are also identical.
 *
 * TODO: hoist out test cases into functions shared between TaskSetSpec and TaskSetBuilderSpecto
 * avoid code duplication.
 */

object TaskSetBuilderSpec {

  import TaskSetSpec._
  import Dsl._

  val TweetSource = TypedTsv[(Long, String)]("LoadTweets.tsv")
  val UserTableSource = TypedTsv[(Long, String, String)]("LoadUserTable.tsv")
  val FollowSource = TypedTsv[(Long, List[Long], List[Long])]("LoadFollowings.tsv")
  val ErroneousSource = TypedTsv[(Long, Double)]("LoadErroneousFeature.tsv")

  abstract class UserTasks extends TaskSetBuilder[Long] {
    override val groupByKey = UserId
  }

  object UsersTableTasks extends UserTasks {
    Load(UserTableSource -> (UserId, ScreenName, Bio)) { args: (Long, String, String) =>
      args
    }
  }

  object TweetTasks extends UserTasks {

    Load((TweetSource, MaxTweetLen) -> (UserId, Tweets)) { args: ((Long, String), Option[Int]) =>
      val ((userId, tweet), maxTweetLen) = args
      val tweetTrimmed = maxTweetLen match {
        case None => tweet
        case Some(len) => tweet.take(len)
      }
      (userId, List(tweetTrimmed))
    }

    Compute(Tweets -> SpamTweets) { tweets: List[String] =>
      tweets
        .toSet
        .filter(_ == "spam1")
    }

    Compute(Tweets -> SpamTweets) { tweets: List[String] =>
      tweets
        .toSet
        .filter(_ == "spam2")
    }

  }

  object FollowTasks extends UserTasks {

    Load((FollowSource, MaxFollowings) -> (UserId, Followees, Followers)) {
        args: ((Long, List[Long], List[Long]), Int) =>
      val ((userId, followees, followers), maxFollowings) = args
      (userId, followees.take(maxFollowings), followers.take(maxFollowings))
    }

    Compute(Followees -> NumFollowees) { followees: List[Long] =>
      followees.size
    }

    Compute(Followers -> NumFollowers) { followers: List[Long] =>
      followers.size
    }

  }

  object ErroneousTasks extends UserTasks {

    Load(ErroneousSource -> (UserId, ErroneousFeature)) { args: (Long, Double) =>
      args
    }

    Compute(CycleFeature2 -> CycleFeature1) { value: Double => value }

    Compute(CycleFeature1 -> CycleFeature2) { value: Double => value }

  }

  object Classifiers extends UserTasks {

    Compute((Bio, ScreenName, Tweets) -> IsBusiness) {
        args: (Option[String], Option[String], Option[List[String]]) =>
      args match {
        case (Some(bio), Some(sn), Some(head :: tail)) => 1.0
        case _ => 0.0
      }
    }

    Compute((Tweets, Keywords) -> IsSpam) {
        args: (Option[List[String]], Set[String]) =>
      val (tweetsOption, keywords) = args
      val tweets = tweetsOption.getOrElse(List[String]())
      if ((tweets.toSet & keywords).isEmpty) {
        0.0
      } else {
        1.0
      }
    }

    Compute((Tweets, NumFollowers, NumFollowees) -> IsEngaged) {
        args: (Option[List[String]], Option[Int], Option[Int]) =>
      args match {
        case (Some(head :: tail), Some(numFollowers), Some(numFollowees)) => {
          if (numFollowers > 0 && numFollowees > 0) {
            1.0
          } else {
            0.0
          }
        }
        case _ => 0.0
      }
    }

  }

  /**
   * User is the TaskSetBuilder that joins together the others
   ************************************************************************************************/
  object UserBuilder extends UserTasks {
    include(
      Classifiers,
      ErroneousTasks,
      FollowTasks,
      TweetTasks,
      UsersTableTasks)
  }

}


/**
 * Scalding jobs
 **************************************************************************************************/

class BuilderScreenNameJob(args: Args) extends Job(args) {
  import TaskSetBuilderSpec._
  import TaskSetSpec._
  UserBuilder.get(args, UserId, ScreenName)
    .write(TypedTsv[(Option[Long], Option[String])]("output.tsv"))
}

class BuilderTweetsJob(args: Args) extends Job(args) {
  import TaskSetBuilderSpec._
  import TaskSetSpec._
  UserBuilder.get(args, UserId, Tweets)
    .write(TypedTsv[(Option[Long], Option[List[String]])]("output.tsv"))
}

class BuilderFollowersScreenNameJob(args: Args) extends Job(args) {
  import TaskSetBuilderSpec._
  import TaskSetSpec._
  UserBuilder.get(args, UserId, Followers, ScreenName)
    .write(TypedTsv[(Option[Long], Option[List[Long]], Option[String])]("output.tsv"))
}

class BuilderComputeNumFollowersJob(args: Args) extends Job(args) {
  import TaskSetBuilderSpec._
  import TaskSetSpec._
  UserBuilder.get(args, UserId, ScreenName, NumFollowers)
    .write(TypedTsv[(Option[Long], Option[String], Option[Int])]("output.tsv"))
}

class BuilderComputeSpamTweetsJob(args: Args) extends Job(args) {
  import TaskSetBuilderSpec._
  import TaskSetSpec._
  UserBuilder.get(args, UserId, SpamTweets)
    .write(TypedTsv[(Option[Long], Option[Set[String]])]("output.tsv"))
}

class BuilderIsBusinessJob(args: Args) extends Job(args) {
  import TaskSetBuilderSpec._
  import TaskSetSpec._
  UserBuilder.get(args, UserId, IsBusiness)
    .write(TypedTsv[(Option[Long], Option[Double])]("output.tsv"))
}

class BuilderIsSpamJob(args: Args) extends Job(args) {
  import TaskSetBuilderSpec._
  import TaskSetSpec._
  UserBuilder.get(args, UserId, IsSpam)
    .write(TypedTsv[(Option[Long], Option[Double])]("output.tsv"))
}

class BuilderIsEngagedJob(args: Args) extends Job(args) {
  import TaskSetBuilderSpec._
  import TaskSetSpec._
  UserBuilder.get(args, UserId, IsEngaged)
    .write(TypedTsv[(Option[Long], Option[Double])]("output.tsv"))
}

class BuilderErroneousFeatureJob(args: Args) extends Job(args) {
  import TaskSetBuilderSpec._
  import TaskSetSpec._
  UserBuilder.get(args, UserId, ErroneousFeature)
    .write(TypedTsv[(Option[Long], Option[Double])]("output.tsv"))
}

class BuilderErroneousCycleJob(args: Args) extends Job(args) {
  import TaskSetBuilderSpec._
  import TaskSetSpec._
  UserBuilder.get(args, UserId, CycleFeature1)
    .write(TypedTsv[(Option[Long], Option[Double])]("output.tsv"))
}

class BuilderErroneousOrphanJob(args: Args) extends Job(args) {
  import TaskSetBuilderSpec._
  import TaskSetSpec._
  UserBuilder.get(args, UserId, OrphanFeature)
    .write(TypedTsv[(Option[Long], Option[Double])]("output.tsv"))
}

/**
 * Tests
 **************************************************************************************************/
@RunWith(classOf[JUnitRunner])
class TaskSetBuilderSpec extends FlatSpec with ShouldMatchers {

  import TaskSetSpec._
  import Dsl._

  /**
   * TaskSetBuilder.get
   ************************************************************************************************/

   "BuilderScreenNameJob" should "fetch two features from a single source" in {

    type OutputTuple = (Option[Long], Option[String])

    val expectedOutput: Set[OutputTuple] = Set(
      (Some(1L), Some("foo")),
      (Some(2L), Some("baz")))

    testJob("BuilderScreenNameJob", args = Map())
      .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
        buf.toSet should equal (expectedOutput)
      }
      .run
      .finish
  }

  "BuilderTweetsJob" should
    "reduce multiple tweet records into a single Tweets feature and use a configuration value" in {

    type OutputTuple = (Option[Long], Option[List[String]])

    val expectedOutput: Set[OutputTuple] = Set(
      (Some(1L), Some(List("s", "s", "c"))),
      (Some(2L), Some(List("s", "p"))),
      (Some(3L), Some(List("b"))))

    testJob(
        jobName ="BuilderTweetsJob",
        args = Map("MaxTweetLen" -> List("1")))
      .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
        buf.toSet should equal (expectedOutput)
      }
      .run
      .finish
  }

  "BuilderFollowersScreenNameJob" should "fetch and join three features from multiple sources" in {

    type OutputTuple = (Option[Long], Option[List[Long]], Option[String])

    val expectedOutput: Set[OutputTuple] = Set(
      (Some(1L), Some(List(4L, 5L)), Some("foo")),
      (Some(2L), None, Some("baz")),
      (Some(3L), Some(List(6L)), None))

    testJob(
        jobName = "BuilderFollowersScreenNameJob",
        args = Map("MaxFollowings" -> List("2")))
      .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
        buf.toSet should equal (expectedOutput)
      }
      .run
      .finish
  }

  "BuilderComputeNumFollowersJob" should "compute a simple feature" in {

    type OutputTuple = (Option[Long], Option[String], Option[Int])

    val expectedOutput: Set[OutputTuple] = Set(
      (Some(1L), Some("foo"), Some(3)),
      (Some(2L), Some("baz"), None),
      (Some(3L), None, Some(1)))

    testJob(
        jobName = "BuilderComputeNumFollowersJob",
        args = Map("MaxFollowings" -> List("10")))
      .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
        buf.toSet should equal (expectedOutput)
      }
      .run
      .finish
  }

  "BuilderComputeSpamTweetsJob" should
    ("yield a single feature that is computed by two distinct compute tasks, " +
     "and reduced via the feature's semigroup") in {

    type OutputTuple = (Option[Long], Option[Set[String]])

    val expectedOutput: Set[OutputTuple] = Set(
      (Some(1L), Some(Set("spam1", "spam2"))),
      (Some(2L), Some(Set("spam2"))),
      (Some(3L), Some(Set())))

    testJob(jobName = "BuilderComputeSpamTweetsJob", args = Map())
      .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
        buf.toSet should equal (expectedOutput)
      }
      .run
      .finish
  }

  "BuilderIsBusinessJob" should "compute a feature that is derived from multiple sources" in {

    type OutputTuple = (Option[Long], Option[Double])

    val expectedOutput: Set[OutputTuple] = Set(
      (Some(1L), Some(1.0)),
      (Some(2L), Some(1.0)),
      (Some(3L), Some(0.0)))

    testJob(jobName = "BuilderIsBusinessJob", args = Map())
      .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
        buf.toSet should equal (expectedOutput)
      }
      .run
      .finish
  }

  "BuilderIsEngagedJob" should
    "compute a feature that as function of other computed features and a loaded feature" in {

    type OutputTuple = (Option[Long], Option[Double])

    val expectedOutput: Set[OutputTuple] = Set(
      (Some(1L), Some(1.0)),
      (Some(2L), Some(0.0)),
      (Some(3L), Some(1.0)))

    testJob(
        jobName = "BuilderIsEngagedJob",
        args = Map("MaxFollowings" -> List("10")))
      .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
        buf.toSet should equal (expectedOutput)
      }
      .run
      .finish
  }

  /**
   * calls to TaskSetBuilder.get that raise an exception.
   * TODO: is there a way to silence the error messages caused by these test cases?
   ************************************************************************************************/

  "BuilderErroneousFeatureJob" should
    "throw a FlowException caused by a DuplicateFeatureException exception" in {

    type OutputTuple = (Option[Long], Option[Double])

    evaluating {
      testJob(
          jobName = "BuilderErroneousFeatureJob",
          args = Map())
        .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
          buf.toSet should equal (Set())
        }
        .run
        .finish
    } should produce [cascading.flow.FlowException]
  }

  "BuilderErroneousCycleJob" should
    "throw a InvocationTargetException caused by a CycleException exception" in {

    type OutputTuple = (Option[Long], Option[Double])

    evaluating {
      testJob(
          jobName = "BuilderErroneousCycleJob",
          args = Map())
        .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
          buf.toSet should equal (Set())
        }
        .run
        .finish
    } should produce [java.lang.reflect.InvocationTargetException]
  }

  "BuilderComputeNumFollowersJob" should
    "throw a InvocationTargetExcept exception if MaxFollowings isn't specified" in {

    type OutputTuple = (Option[Long], Option[String], Option[Int])

    evaluating {
      testJob(
          jobName = "BuilderComputeNumFollowersJob",
          args = Map())
        .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
          buf.toSet should equal (Set())
        }
        .run
        .finish
    } should produce [java.lang.reflect.InvocationTargetException]
  }

  "BuilderErroneousOrphanJob" should
    "throw a InvocationTargetException caused by a NoLoadTasksException exception" in {

    type OutputTuple = (Option[Long], Option[Double])

    evaluating {
      testJob(
          jobName = "BuilderErroneousOrphanJob",
          args = Map())
        .sink[OutputTuple](TypedTsv[OutputTuple]("output.tsv")) { buf =>
          buf.toSet should equal (Set())
        }
        .run
        .finish
    } should produce [java.lang.reflect.InvocationTargetException]
  }

}
