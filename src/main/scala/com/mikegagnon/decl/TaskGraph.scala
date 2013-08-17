package com.mikegagnon.decl

/**
 * Task A points to Task B iff Task B depends on an argument that Task A produces
 */
class TaskGraph(override val nodes: Set[Task]) extends Dag[Task] {

  // First step: map each argument to the set of tasks that accept that argument
  val requires: Map[Argument, Set[Task]] = nodes
    .flatMap { task =>
      task.inputArgs.map{ arg: Argument => (arg, task: Task) }
    }
    .groupBy { _._1 }
    .mapValues { pairs: Set[(Argument, Task)] =>
      pairs.map{ _._2 }
    }

  override val edges: Set[(Task, Task)] = for {
      fromTask <- nodes
      feature <- fromTask.outputFeatures
      toTask <- requires.getOrElse(feature, Set())
    } yield (fromTask, toTask)
}
