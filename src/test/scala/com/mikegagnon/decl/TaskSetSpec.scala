package com.mikegagnon.decl

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

import com.twitter.scalding.Args

object TaskSetSpec {
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
  val LoadUserTable = DummyLoadTask(inputConfig=Set(),
                            outputFeatures=Set(UserId, ScreenName, Bio))
  val LoadFollowings = DummyLoadTask(inputConfig=Set(MaxFollowings),
                            outputFeatures=Set(UserId, Followees, Followers))
  val LoadTweets = DummyLoadTask(inputConfig=Set(MaxTweetLen),
                            outputFeatures=Set(UserId, Tweets))

  /**
   * Compute tasks
   ************************************************************************************************/
  val ComputeFollowCounts = DummyComputeTask(inputConfig=Set(),
                                  inputFeatures=Set(Followees, Followers),
                                  outputFeatures=Set(NumFollowers, NumFollowees))
  val ComputeBusiness = DummyComputeTask(inputConfig=Set(),
                                  inputFeatures=Set(Bio, ScreenName, Tweets),
                                  outputFeatures=Set(IsBusiness))
  val ComputeIsSpam = DummyComputeTask(inputConfig=Set(Keywords),
                                  inputFeatures=Set(Tweets),
                                  outputFeatures=Set(IsSpam))
  val ComputeEngaged = DummyComputeTask(inputConfig=Set(),
                                  inputFeatures=Set(Tweets, NumFollowers, NumFollowees),
                                  outputFeatures=Set(IsEngaged))
  /**
   * TaskSet
   ************************************************************************************************/

  val dummyTasks = Set[Task](
    LoadUserTable,
    LoadFollowings,
    LoadTweets,
    ComputeFollowCounts,
    ComputeBusiness,
    ComputeIsSpam,
    ComputeEngaged)
}

/**
 * Tests
 **************************************************************************************************/
@RunWith(classOf[JUnitRunner])
class TaskSetSpec extends FlatSpec with ShouldMatchers {

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
}
