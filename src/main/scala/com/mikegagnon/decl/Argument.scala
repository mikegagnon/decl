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

import com.twitter.algebird.Semigroup
import com.twitter.scalding.Args

import scala.reflect.Manifest

/**
 * Uses a type member instead of a type parameter in order for type classes.
 */
trait Argument extends Serializable {
  type V
}

/**
 * A configuration value holds a global unit of information that is initialized once, before any
 * Features are loaded or computed.
 */
abstract class ConfigValue[X] extends Argument {
  type V = X
  def init(args: Args): V
}

/**
 * A feature is akin to a Scalding field.
 */
abstract class FeatureBase[X] extends Argument {
  type V = X
  val semi: Option[Semigroup[V]]
}

class DuplicateFeatureException(msg: String) extends IllegalArgumentException(msg)

/**
 * It is a runtime error (DuplicateFeatureException) if an individual record has several values for
 * the same feature.
 */
abstract class Feature[V] extends FeatureBase[V] {
  override val semi = None
}

/**
 * If an individual record has duplicate values for the same FeatureWithReducer then the values
 * will be merged using semi
 */
abstract class FeatureWithReducer[V: Semigroup] extends FeatureBase[V] {
  override val semi = Some(implicitly[Semigroup[V]])
}
