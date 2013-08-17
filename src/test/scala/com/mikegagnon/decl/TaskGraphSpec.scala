package com.mikegagnon.decl

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

import com.twitter.scalding.{Args, Mode, TypedPipe}
import cascading.flow.FlowDef

/**
 * This test suite does not attempt to thoroughly test the Dag functionality; that is the job of
 * DagSpec.scala. This suite simply tests
 *
 * This spec only needs to test the TaskGraph mapping from Tasks to nodes and edges.
 */
case class DummyLoadTask(inputConfig: Set[ConfigValue[_]], outputFeatures: Set[FeatureBase[_]])
    extends LoadTask {
  override def load(config: ConfigMap)
    (implicit flowDef: FlowDef, mode: Mode): TypedPipe[FeatureMap] = null
}

case class DummyComputeTask(inputConfig: Set[ConfigValue[_]], inputFeatures: Set[FeatureBase[_]],
    outputFeatures: Set[FeatureBase[_]]) extends ComputeTask {
  override def compute(features: FeatureMap, env: ConfigMap): FeatureMap = null
}

@RunWith(classOf[JUnitRunner])
class TaskGraphSpec extends FlatSpec with ShouldMatchers {

  abstract class DummyFeature extends Feature[Int]
  object FeatureA extends DummyFeature
  object FeatureB extends DummyFeature
  object FeatureC extends DummyFeature
  object FeatureD extends DummyFeature
  object FeatureE extends DummyFeature
  object FeatureF extends DummyFeature
  object FeatureG extends DummyFeature
  object FeatureH extends DummyFeature
  object FeatureI extends DummyFeature
  object FeatureJ extends DummyFeature
  object FeatureK extends DummyFeature

  abstract class DummyConfigVal extends ConfigValue[Int] { def init(s: Args) = 1 }
  object DummyConfigVal1 extends DummyConfigVal
  object DummyConfigVal2 extends DummyConfigVal
  object DummyConfigVal3 extends DummyConfigVal
  object DummyConfigVal4 extends DummyConfigVal
  object DummyConfigVal5 extends DummyConfigVal

  val load1 = DummyLoadTask(inputConfig=Set(),
                            outputFeatures=Set(FeatureA, FeatureB))
  val load2 = DummyLoadTask(inputConfig=Set(DummyConfigVal1),
                            outputFeatures=Set(FeatureC))
  val load3 = DummyLoadTask(inputConfig=Set(DummyConfigVal2, DummyConfigVal3),
                            outputFeatures=Set(FeatureA, FeatureD))
  val compute1 = DummyComputeTask(inputConfig=Set(DummyConfigVal4),
                                  inputFeatures=Set(FeatureA),
                                  outputFeatures=Set(FeatureE))
  val compute2 = DummyComputeTask(inputConfig=Set(),
                                  inputFeatures=Set(FeatureB, FeatureC),
                                  outputFeatures=Set(FeatureE, FeatureF))
  val compute3 = DummyComputeTask(inputConfig=Set(),
                                  inputFeatures=Set(FeatureE),
                                  outputFeatures=Set(FeatureF, FeatureG, FeatureH))
  val compute4 = DummyComputeTask(inputConfig=Set(DummyConfigVal5),
                                  inputFeatures=Set(FeatureH),
                                  outputFeatures=Set(FeatureK))

  val tasks: Set[Task] = Set(load1, load2, load3, compute1, compute2, compute3, compute4)
  val graph = new TaskGraph(tasks)

  def checkSorted(graph: TaskGraph): Unit = {
    DagSpec.sorted(graph.sorted, graph) should equal (true)
  }

  "TaskGraph" should "be sorted" in {
    checkSorted(graph)
  }

  it should "compute ancestors correctly for load1" in {
    graph.ancestors(load1) should equal (Set())
  }

  it should "compute ancestors correctly for load2" in {
    graph.ancestors(load2) should equal (Set())
  }

  it should "compute ancestors correctly for load3" in {
    graph.ancestors(load3) should equal (Set())
  }

  it should "compute ancestors correctly for compute1" in {
    graph.ancestors(compute1) should equal (Set(load1, load3))
  }

  it should "compute ancestors correctly for compute2" in {
    graph.ancestors(compute2) should equal (Set(load1, load2))
  }

  it should "compute ancestors correctly for compute3" in {
    graph.ancestors(compute3) should equal (Set(load1, load2, load3, compute1, compute2))
  }

  it should "compute ancestors correctly for compute4" in {
    graph.ancestors(compute4) should equal (Set(load1, load2, load3, compute1, compute2, compute3))
  }
}
