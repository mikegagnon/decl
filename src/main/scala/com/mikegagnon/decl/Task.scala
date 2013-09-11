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

  lazy val inputArgs: Set[Argument] =
    inputConfig.map{ a => a: Argument } ++ inputFeatures.map{ a => a: Argument }
}
