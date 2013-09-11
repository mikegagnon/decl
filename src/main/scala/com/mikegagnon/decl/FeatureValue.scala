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

/**
 * The FeatureValue class is useful so that a FeatureMap can hold a collection of features in such a
 * way that Monoid.plus(featureMap1, featureMap2) will merge the individual features using pair-wise
 * semigroup operations.
 *
 * Note: It would be more type-safe if we could define this class as:
 *    FeatureValue[T](feature: FeatureBase[T], value: T)
 * However, I do not think this type safety is possible based on my use case. The problem is
 * that the feature map monoid requires a single semigroup for the values
 * (the FeatureValueSemigroup), which must be able to operate on all feature values in the feature
 * map. But if FeatureValue took a type T, then the FeatureValueSemigroup would also need to take
 * a type T (essentially there would be a different FeatureValueSemigroup for each type T), and
 * there wouldn't be a single definitive FeatureValueSemigroup which you could pass to the Map
 * Monoid.
 */
object FeatureValue {
  implicit val semi = new FeatureValueSemigroup
}

case class FeatureValue(feature: FeatureBase[_], value: Any)

class FeatureValueSemigroup extends Semigroup[FeatureValue] {
  override def plus(left: FeatureValue, right: FeatureValue) = {
    require(left.feature == right.feature, ("The FeatureValue's features do not match: " +
        "left.feature == %s, right.feature == %s").format(left.feature, right.feature))
    left.feature.semi match {

      // TODO: consider making a "relaxed" config option, that simply ignores duplicates
      case None => throw new DuplicateFeatureException(("Cannot merge feature values %s and %s " +
        "because they are not defined as FeatureWithReducer[V]. Possible causes of error: " +
        "(1) You are loading (or computing) the same feature multiple times, or " +
        "(2) You actually intend values for this feature to be reduciible, in which case you" +
        "should define the feature as a FeatureWithReducer[V] instead of Feature[V], or" +
        "(3) Your data contains erroneous duplicate values.")
        .format(left, right))

      case Some(semi) => FeatureValue(left.feature, semi.plus(left.value, right.value))
    }
  }
}
