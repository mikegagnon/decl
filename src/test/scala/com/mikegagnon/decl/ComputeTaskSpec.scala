package com.mikegagnon.decl

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

@RunWith(classOf[JUnitRunner])
class ComputeTaskSpec extends FlatSpec with ShouldMatchers {

  import TaskSetSpec._

  /**
   * Test cases with one input, one output
   ************************************************************************************************/

  "ComputeTask.apply" should
      "work with 1 required input feature and one required output feature" in {
    val computeTask = ComputeTask(ScreenName -> IsSpam) { sn: String =>
      if (sn == "foo") {
        1.0
      } else {
        0.0
      }
    }

    // the required argument is missing
    computeTask.compute(FeatureMap(), ConfigMap()) should equal (FeatureMap())

    // the required argument is present
    computeTask.compute(FeatureMap(
      ScreenName -> "a"), ConfigMap()) should equal (FeatureMap(IsSpam -> 0.0))
    computeTask.compute(FeatureMap(
      ScreenName -> "foo"), ConfigMap()) should equal (FeatureMap(IsSpam -> 1.0))
  }

  it should "work with 1 optional input feature and one required output feature" in {
    val computeTask = ComputeTask(ScreenName -> IsSpam) { sn: Option[String] =>
      sn.map { s => 1.0 }.getOrElse(0.0)
    }

    // the optional argument is missing
    computeTask.compute(FeatureMap(), ConfigMap()) should equal (FeatureMap(IsSpam -> 0.0))

    // the optional argument is present
    computeTask.compute(FeatureMap(
      ScreenName -> "a"), ConfigMap()) should equal (FeatureMap(IsSpam -> 1.0))
  }

  it should "work with 1 required input feature and one optional output feature" in {
    val computeTask = ComputeTask(ScreenName -> IsSpam) { sn: String =>
      if (sn == "foo") {
        Some(1.0)
      } else {
        None
      }
    }

    // the required argument is missing
    computeTask.compute(FeatureMap(), ConfigMap()) should equal (FeatureMap())

    // the required argument is present, but results in a None return value
    computeTask.compute(FeatureMap(ScreenName -> "a"), ConfigMap()) should equal (FeatureMap())

    // the required argument is present, and results in a Some(1.0) return value
    computeTask.compute(FeatureMap(
      ScreenName -> "foo"), ConfigMap()) should equal (FeatureMap(IsSpam -> 1.0))

  }

  it should "work with 1 optional input feature and one optional output feature" in {
    val computeTask = ComputeTask(ScreenName -> IsSpam) { sn: Option[String] =>
      sn.map{ s => 1.0 }
    }

    // the optional argument is missing
    computeTask.compute(FeatureMap(), ConfigMap()) should equal (FeatureMap())

    // the optional argument is present
    computeTask.compute(FeatureMap(
      ScreenName -> "a"), ConfigMap()) should equal (FeatureMap(IsSpam -> 1.0))
  }

  /**
   * Test cases with a variety of inputs and outputs
   ************************************************************************************************/

  it should "work with 2 required features, one config value, and one required output feature" in {
    val computeTask = ComputeTask((ScreenName, Tweets, Keywords) -> IsSpam) {
        args: (String, List[String], Set[String]) =>
      val (sn, tweets, keywords) = args
      (sn :: tweets)
        .map { word =>
         if (keywords(word)) {
          1.0
         } else {
          0.0
         }
        }
        .sum
    }
    val configMap = ConfigMap(Keywords -> Set("a", "b", "c"))

    // all required arguments are missing
    computeTask.compute(FeatureMap(), configMap) should equal (FeatureMap())

    // missing one required argument
    computeTask.compute(FeatureMap(ScreenName -> "a"), configMap) should equal (FeatureMap())
    computeTask.compute(FeatureMap(Tweets -> List("b", "a")), configMap) should equal (FeatureMap())

    // all required arguments are present
    computeTask.compute(FeatureMap(
      ScreenName -> "a",
      Tweets -> List("b", "x")), configMap) should equal (FeatureMap(IsSpam -> 2.0))
  }

  it should ("work with " +
      "input of (1 optional feature, 1 required feature, 1 config value), and" +
      "output of (2 optional features and 1 required feature)") in {
    val computeTask =
      ComputeTask((ScreenName, Tweets, Keywords) -> (IsSpam, SpamTweets, IsEngaged)) {
          args: (Option[String], List[String], Set[String]) =>
        val (sn, tweets, keywords) = args
        val spamTweets = (sn.getOrElse("") :: tweets)
          .filter { word => keywords(word) }
          .toSet
        val (isSpam, isEngaged) = if (spamTweets.isEmpty) {
          (None, None)
        } else {
          (Some(spamTweets.size.toDouble), Some(-1.0 * spamTweets.size.toDouble))
        }

        (isSpam, spamTweets, isEngaged)
      }

    val configMap = ConfigMap(Keywords -> Set("a", "b", "c"))

    // all features are missing
    computeTask.compute(FeatureMap(), configMap) should equal (FeatureMap())

    // missing the required argument
    computeTask.compute(FeatureMap(ScreenName -> "a"), configMap) should equal (FeatureMap())

    // missing the optional argument
    computeTask.compute(
      FeatureMap(
        Tweets -> List("b", "x", "y")), configMap) should equal (
      FeatureMap(
        IsSpam -> 1.0,
        IsEngaged -> -1.0,
        SpamTweets -> Set("b")))

    // not missing any arguments
    computeTask.compute(
      FeatureMap(
        ScreenName -> "a",
        Tweets -> List("b", "x", "y")), configMap) should equal (
      FeatureMap(
        IsSpam -> 2.0,
        IsEngaged -> -2.0,
        SpamTweets -> Set("a", "b")))

    // not missing any arguments and returns None features
    computeTask.compute(
      FeatureMap(
        ScreenName -> "x",
        Tweets -> List("q", "x", "y")), configMap) should equal (
      FeatureMap(
        SpamTweets -> Set[String]()))
  }
}
