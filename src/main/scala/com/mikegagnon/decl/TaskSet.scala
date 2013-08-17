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
}
