package com.mikegagnon.decl

/**
 * Tasks are used to either load or compute new features.
 *
 * In and Out contain the input & output types for this tasks.
 */
trait Task extends Serializable {

  // The set of environment values that this task depends upon
  val inputConfig: Set[ConfigValue[_]]

  // The set of features that this task depnds upon
  val inputFeatures: Set[FeatureBase[_]]

  // Every task outputs one or more features; this specifies the features this task output
  val outputFeatures: Set[FeatureBase[_]]

  lazy val input: Set[Argument] =
    inputConfig.map{ a => a: Argument } ++ inputFeatures.map{ a => a: Argument }
}
