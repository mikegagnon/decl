package com.mikegagnon.decl

import com.twitter.algebird.Monoid

object TaskSetBuilder {

  implicit def toTaskSet[V: Ordering](builder: TaskSetBuilder[V]) =
    new TaskSet[V] {
      override val groupByKey = builder.groupByKey
      override val tasks = builder.tasks
    }
}

abstract class TaskSetBuilder[V: Ordering] {

  val groupByKey: Feature[V]
  final var tasks = Set[Task]()

  final def include(tasksets: TaskSet[V]*): TaskSetBuilder[V] = {
    tasksets.foreach { taskset =>
      require(taskset.groupByKey == groupByKey)
    }
    tasks ++= Monoid.sum(tasksets.map{ _.tasks })
    this
  }

  final def Load[InArgs, InTypes, OutFeatures, OutTypes]
      (inOut: (InArgs, OutFeatures))
      (fn: Function1[InTypes, OutTypes])
      (implicit
        input: LoadTaskInput[InArgs, InTypes],
        output: Output[OutFeatures, OutTypes]) = {
    val task = LoadTask(inOut)(fn)
    tasks += task
    task
  }

  final def Compute[InArgs, InTypes, OutFeatures, OutTypes]
      (inOut: (InArgs, OutFeatures))
      (fn: Function1[InTypes, OutTypes])
      (implicit
       input: Input[InArgs, InTypes],
       output: Output[OutFeatures, OutTypes]) = {
    val task = ComputeTask(inOut)(fn)
    tasks += task
    task
  }



}
