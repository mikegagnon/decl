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
