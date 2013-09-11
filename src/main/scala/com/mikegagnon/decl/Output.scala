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

import com.twitter.algebird.Monoid

/**
 * This file contains classes which are useful for "compiling" shorthand syntax for tasks (compute
 * and load tasks) into actual Task objects.
 *
 * Specifically, these classes are used for handling the output features for tasks.
 */

/**
 * OutputToFeatureMap is a type class used to convert an output value into a FeatureMap containing
 * that value.
 *
 * If Feature <: FeatureBase[V], then X is either viewable as V, or viewable as Option[V]
 * The view bounds is useful because then X can be None, which will be converted to Option[V].
 */
abstract class OutputToFeatureMap[Feature <: FeatureBase[_], X] extends Serializable {
   // Convert outValue to a FeatureMap
  def apply(outFeature: Feature, outValue: X): FeatureMap
}

object OutputToFeatureMap {

  implicit def outputRequired[FeatureA <: FeatureBase[A], A <% FeatureA#V] =
    new OutputToFeatureMap[FeatureA, A] {
      def apply(outFeature: FeatureA, outValue: A): FeatureMap =
        FeatureMap(outFeature -> outValue)
    }

  implicit def outputOptional[FeatureA <: FeatureBase[A], O <% Option[FeatureA#V], A <% FeatureA#V] =
    new OutputToFeatureMap[FeatureA, O] {
      def apply(outFeature: FeatureA, outValue: O): FeatureMap =
        outValue.asInstanceOf[Option[FeatureA#V]] match {
          case None => FeatureMap()
          case Some(outValue) => FeatureMap(outFeature -> outValue)
        }
    }
}

/**
 * Output is a type class used to associate a tuple of output feature types (of type OutFeatures)
 * with a tuple of value-types associated with OutFeatures.
 *
 * This type class can be instantiated iff the __subtypes__ of OutFeatures match the types of
 * OutTypes.
 *
 * Example 1:
 * if OutFeatures == (Feature[Int], Feature[String])
 * then:
 *   OutTypes could be (Int, String)
 *      --- or ---
 *   OutTypes could be (Int, Option[String])
 *      --- or ---
 *   OutTypes could be (Option[Int], String)
 *      --- or ---
 *   OutTypes could be (Option[Int], Option[String])
 *
 * Note that the OutputToFeatureMap type class is used so that the contituent types of InTypes
 * may be Option types.
 */
abstract class Output[OutFeatures, OutTypes] extends Serializable {
  def toFeatureMap(outFeatures: OutFeatures, outValues: OutTypes): FeatureMap
  def outputFeatures(outFeatures: OutFeatures): Set[FeatureBase[_]]
}

object Output {

  implicit def output1[FeatureA <: FeatureBase[_], A]
      (implicit outputA: OutputToFeatureMap[FeatureA, A]) =
    new Output[FeatureA, A] {
      def toFeatureMap(outFeature: FeatureA, outValue: A): FeatureMap =
        outputA(outFeature, outValue)
      def outputFeatures(outFeature: FeatureA) = Set(outFeature)
    }

  implicit def output2[
      FeatureA <: FeatureBase[_],
      FeatureB <: FeatureBase[_],
      A,
      B]
      (implicit outputA: OutputToFeatureMap[FeatureA, A],
                outputB: OutputToFeatureMap[FeatureB, B]) =
    new Output[(FeatureA, FeatureB), (A, B)] {
      def toFeatureMap(outFeatures: (FeatureA, FeatureB), outValues: (A, B)): FeatureMap =
        Monoid.plus(outputA(outFeatures._1, outValues._1),
                    outputB(outFeatures._2, outValues._2))
      def outputFeatures(outFeatures: (FeatureA, FeatureB)) = Set(outFeatures._1, outFeatures._2)
    }

  implicit def output3[
      FeatureA <: FeatureBase[_],
      FeatureB <: FeatureBase[_],
      FeatureC <: FeatureBase[_],
      A,
      B,
      C]
      (implicit outputA: OutputToFeatureMap[FeatureA, A],
                outputB: OutputToFeatureMap[FeatureB, B],
                outputC: OutputToFeatureMap[FeatureC, C]) =
    new Output[(FeatureA, FeatureB, FeatureC), (A, B, C)] {
      def toFeatureMap(outFeatures: (FeatureA, FeatureB, FeatureC), outValues: (A, B, C)):
          FeatureMap =
        Monoid.sum(Seq(outputA(outFeatures._1, outValues._1),
                       outputB(outFeatures._2, outValues._2),
                       outputC(outFeatures._3, outValues._3)))
      def outputFeatures(outFeatures: (FeatureA, FeatureB, FeatureC)) =
        Set(outFeatures._1, outFeatures._2, outFeatures._3)
    }
}
