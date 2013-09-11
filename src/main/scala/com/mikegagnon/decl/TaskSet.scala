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

import cascading.flow.FlowDef
import com.twitter.algebird.Semigroup

import com.twitter.scalding.{
  Args,
  Dsl,
  Mode,
  TypedPipe}

class NoLoadTasksException(msg: String) extends Exception(msg)

object TaskSet {

  // initialize the configuration values needed by loadTasks and computeTasks
  def initConfig(
      args: Args,
      loadTasks: Set[LoadTask],
      computeTasks: List[ComputeTask]) : ConfigMap = {

    val runTasks: Set[Task] =
      loadTasks.map{ t => t: Task} ++
      computeTasks.map{t => t: Task}.toSet

    // The set of all environment values that need to be initialized
    val inputConfig: Set[ConfigValue[_]] = runTasks
      .flatMap { task =>
        task.inputConfig
      }

    // Initialize the configuration values
    val configValues = inputConfig
      .map { configValue: ConfigValue[_] =>
        (configValue, configValue.init(args))
      }

    new ConfigMap(configValues.toMap)
  }

  /**
   * Given a universe of tasks, and a set of features to be computed, determine which tasks need to
   * be run, and the ordering of ComputeTasks.
   * The groupByKey should __not__ be included in features (or else every loadTask in the universe
   * would be scheduled)
   */
  def scheduleTasks(tasks: Set[Task], features: Set[FeatureBase[_]])
      : (Set[LoadTask], List[ComputeTask]) = {

    val graph = new TaskGraph(tasks)

    // the set of tasks that directly compute features
    val leaves: Set[Task] = tasks
      .filter { task =>
        (task.outputFeatures & features).nonEmpty
      }

    // The set of all tasks that need to be run in order to compute outputFeatures
    val subgraph = graph.subgraph(leaves)

    // The set of all LoadTasks that need to be run
    val loadTasks: Set[LoadTask] = subgraph
      .nodes
      .flatMap { task =>
        task match {
          case loadTask: LoadTask => Some(loadTask)
          case _ => None
        }
      }

    // The list of all ComputeTasks that need to be run, in execution order
    val computeTasks: List[ComputeTask] = subgraph
      .sorted
      .flatMap { task =>
        task match {
          case computeTask: ComputeTask => Some(computeTask)
          case _ => None
        }
      }

    (loadTasks, computeTasks)
  }

}

abstract class TaskSet[V: Ordering] extends Serializable {

  import Dsl._

  // note: groupByKey must be a feature __without__ a reducer
  val groupByKey: Feature[V]
  val tasks: Set[Task]

  final def include(taskset: TaskSet[V]): TaskSet[V] = {
    require(taskset.groupByKey == groupByKey)
    val myTasks = tasks
    new TaskSet {
      override val groupByKey = taskset.groupByKey
      override val tasks = myTasks ++ taskset.tasks
    }
  }

  final def get[
          Feature1[Value1] <: FeatureBase[_],
          Value1 <: Feature1[Value1]#V]
         (args: Args,
          feature1: Feature1[Value1])
         (implicit flowDef: FlowDef, mode: Mode)
         : TypedPipe[Option[Value1]]
    = run(args, Set(feature1)) { fmap: FeatureMap =>
      fmap.get(feature1)
    }

  final def get[
          Feature1[Value1] <: FeatureBase[_],
          Feature2[Value2] <: FeatureBase[_],
          Value1 <: Feature1[Value1]#V,
          Value2 <: Feature2[Value2]#V]
         (args: Args,
          feature1: Feature1[Value1],
          feature2: Feature2[Value2])
         (implicit flowDef: FlowDef, mode: Mode)
         : TypedPipe[(Option[Value1], Option[Value2])]
    = run(args, Set(feature1, feature2)) { fmap: FeatureMap =>
        (fmap.get(feature1),
         fmap.get(feature2))
    }

  final def get[
          Feature1[Value1] <: FeatureBase[_],
          Feature2[Value2] <: FeatureBase[_],
          Feature3[Value3] <: FeatureBase[_],
          Value1 <: Feature1[Value1]#V,
          Value2 <: Feature2[Value2]#V,
          Value3 <: Feature2[Value3]#V]
         (args: Args,
          feature1: Feature1[Value1],
          feature2: Feature2[Value2],
          feature3: Feature3[Value3])
         (implicit flowDef: FlowDef, mode: Mode)
         : TypedPipe[(Option[Value1], Option[Value2], Option[Value3])]
     = run(args, Set(feature1, feature2, feature3)) { fmap: FeatureMap =>
        (fmap.get(feature1),
         fmap.get(feature2),
         fmap.get(feature3))
    }

  // TODO: add get functions for more than 3 features (perhaps using Ruby macros)

  final private[decl] def run[T](
      args: Args,
      features: Set[FeatureBase[_]])
     (projector: FeatureMap => T)
     (implicit flowDef: FlowDef, mode: Mode): TypedPipe[T] = {

    val outputFeatures = features.filter{ _ != groupByKey }

    val (loadTasks: Set[LoadTask],
        computeTasks: List[ComputeTask]) = TaskSet.scheduleTasks(tasks, outputFeatures)

    val config: ConfigMap = TaskSet.initConfig(args, loadTasks, computeTasks)

    // run the Load tasks
    val loadedPipe: Option[TypedPipe[FeatureMap]] = loadTasks
      // is there a better collection function?
      .foldLeft(None: Option[TypedPipe[FeatureMap]]) { case (pipeOption, loadTask) =>
        pipeOption match {
          case None => Some(loadTask.run(groupByKey, config))
          case Some(pipe) => Some(pipe ++ loadTask.run(groupByKey, config))
        }
      }

    // group and sum the results of the load tasks
    val groupedPipe: TypedPipe[FeatureMap] = loadedPipe match {
      case None => throw new NoLoadTasksException(
        "Cannot compute features because the scheduler did not find any load tasks. " +
        "You are trying to load or compute a feature that this TaskSet doesn't know how to " +
        "load or compute. This can happen for example if you forget to add a task to the TaskSet.")
      case Some(pipe) => pipe
        .map { featureMap: FeatureMap =>
          val key = featureMap.get(groupByKey)
          if (key.isEmpty) {
            throw new RuntimeException((
              "While grouping the loaded features, encountered a FeatureMap that is missing the " +
              "groupByKey == %s.").format(groupByKey))
          }
          (key.get, featureMap)
        }
        .group
        .reduce { (left, right) =>
          // Do not include the groupByKey in the Semigroup.plus because there will be duplicate
          // values for groupByKey and we do not want to sum those values
          val sumFeatureMap = Semigroup.plus(left - groupByKey, right - groupByKey)
          sumFeatureMap + (groupByKey -> left.get(groupByKey).get)
        }
        .map { case (key, featureMap) =>
          featureMap
        }
    }

    // run the compute tasks
    val computedPipe: TypedPipe[FeatureMap] = groupedPipe
      .map { featureMap: FeatureMap =>
        computeTasks.foldLeft(featureMap) { case (featureMap: FeatureMap, task: ComputeTask) =>
          Semigroup.plus(featureMap, task.compute(featureMap, config))
        }
      }

    // project each featureMap to a value of type T
    computedPipe.map(projector)
  }
}

