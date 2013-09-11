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
 * A config map is simply a typesafe Map from ConfigValue objects to values.
 *
 * Example of correct usage:
 *    ConfigMap(MaxTweetLen -> 140, Keywords -> Set("foo"))
 *
 * whereas the following incorrect usage is a compile error (because MaxTweetLen should be an int):
 *    ConfigMap(MaxTweetLen -> "foo")
 *
 * Note: ConfigMap has no need for a Monoid (unlike FeatureMap) because when we want to combine
 * config maps, we want the values on the right to override the values on the left (which is
 * the behavior of ++)
 */
object ConfigMap {

  def apply(): ConfigMap =
    new ConfigMap()

  def apply[A](a: (ConfigValue[A], A)): ConfigMap =
    new ConfigMap(Map(a))

  def apply[A, B](a: (ConfigValue[A], A),
                  b: (ConfigValue[B], B)): ConfigMap =
    new ConfigMap(Map(a, b))

  def apply[A, B, C](a: (ConfigValue[A], A),
                     b: (ConfigValue[B], B),
                     c: (ConfigValue[C], C)): ConfigMap =
    new ConfigMap(Map(a, b, c))
}

class ConfigMap(val map: Map[ConfigValue[_], Any] = Map()) {

  def apply[C[V] <: ConfigValue[_], V <: C[V]#V](configValue: C[V]): V =
    map(configValue).asInstanceOf[V]

  def +[V](kv: (ConfigValue[V], V)): ConfigMap = new ConfigMap(map + kv)

  def ++(that: ConfigMap) = new ConfigMap(map ++ that.map)

  override def equals(that: Any) = that match {
    case that: ConfigMap => this.map == that.map
    case _ => false
  }

  override def hashCode = map.hashCode
}
