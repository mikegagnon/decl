package com.mikegagnon.decl

import com.twitter.scalding.Args

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
