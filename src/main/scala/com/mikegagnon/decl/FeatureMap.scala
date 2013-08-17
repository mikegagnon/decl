package com.mikegagnon.decl

import com.twitter.algebird.Monoid

/**
 * A feature map is simply a typesafe Map from Feature objects to values.
 *
 * Example of correct usage:
 *    FeatureMap(UserId -> 1L, Bio -> "foo")
 *
 * whereas the following incorrect usage is a compile time error (because Bio should be a string):
 *    FeatureMap(UserId -> 1L, Bio -> 5)
 *
 * The Monoid for FeatureMap performs pairwise sums for each feature, using the semigroup associated
 * with each feature.
 *
 * Example of correct usage:
 *    Monoid.sum(FeatureMap(Bio -> "foo",
 *                          Tweets -> List("a")),
 *               FeatureMap(ScreenName -> "bob",
 *                          Tweets -> List("b")))
 *            == FeatureMap(ScreenName -> "bob",
 *                          Bio -> "foo",
 *                          Tweets -> List("a", "b"))
 */
object FeatureMap {

  def wrap[V](kv: (FeatureBase[V], V)): (FeatureBase[_], FeatureValue) = {
    val (feature, value) = kv
    (feature -> FeatureValue(feature, value))
  }

  def apply(): FeatureMap =
    new FeatureMap()

  def apply[A](a: (FeatureBase[A], A)): FeatureMap =
    new FeatureMap(Map(wrap(a)))

  def apply[A, B](a: (FeatureBase[A], A),
                  b: (FeatureBase[B], B)): FeatureMap =
    new FeatureMap(Map(wrap(a), wrap(b)))

  def apply[A, B, C](a: (FeatureBase[A], A),
                     b: (FeatureBase[B], B),
                     c: (FeatureBase[C], C)): FeatureMap =
    new FeatureMap(Map(wrap(a), wrap(b), wrap(c)))

  implicit def monoid = new Monoid[FeatureMap] {
    def zero = new FeatureMap()
    def plus(left: FeatureMap, right: FeatureMap) = {
      new FeatureMap(Monoid.plus(left.map, right.map))
    }
  }
}

class FeatureMap(val map: Map[FeatureBase[_], FeatureValue] = Map()) {

  def get[F[V] <: FeatureBase[_], V <: F[V]#V](feature: F[V]): Option[V] = {
    map.get(feature).map{ _.value.asInstanceOf[V] }
  }

  def +[V](kv: (FeatureBase[V], V)): FeatureMap = new FeatureMap(map + FeatureMap.wrap(kv))

  def -[V](k: FeatureBase[V]): FeatureMap = new FeatureMap(map - k)

  override def equals(that: Any) = that match {
    case that: FeatureMap => this.map == that.map
    case _ => false
  }

  override def hashCode = map.hashCode
}
