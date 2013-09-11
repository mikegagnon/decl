package com.mikegagnon.decl

import com.twitter.scalding.Mappable

/**
 * LoadTaskInput is a type class that provides similar functionality to the Input tyep class,
 * except LoadTaskInput is used for providing input to a LoadTask.
 *
 * This type class can be instantiated iff the __subtypes__ of InArgs match the types of InTypes.
 * Furthermore, InArgs can be either:
 *   - a Mappable source, or
 *   - a tuple that begins with a Mappable source, followed by ConfigValue objects
 *
 * Example:
 * if InArgs == (TypedTsv[(Int, String)], ConfigValue[Double])
 * then: InTypes must be ((Int, String), Double)
 */
abstract class LoadTaskInput[InArgs, InTypes] extends Serializable {

  /**
   * The inner-type associated with the Mappable source.
   * E.g. if InArgs == (TypedTsv[(Int, String)], ConfigValue[Double])
   * then SourceType == (Int, String)
   */
  type SourceType

  /**
   * return sourceValue along with any configuration values specified in inArgs.
   * configuration values are extracted from config.
   */
  def argsToTuple(sourceValue: SourceType, config: ConfigMap, inArgs: InArgs): InTypes

  // The subset of inArgs that are ConfigValue objects
  def inputConfig(inArgs: InArgs): Set[ConfigValue[_]]

  // Extract the mappable object from inArgs
  def getSource(inArgs: InArgs): Mappable[SourceType]
}

object LoadTaskInput {

  implicit def input1[ArgA <: Mappable[A], A] =
    new LoadTaskInput[ArgA, A] {
      type SourceType = A
      def argsToTuple(sourceValue: SourceType, config: ConfigMap, inArg: ArgA) = sourceValue
      def inputConfig(inArg: ArgA) = Set[ConfigValue[_]]()
      def getSource(inArg: ArgA) = inArg
    }

  implicit def input2[ArgA <: Mappable[A],
                      ArgB <: ConfigValue[B],
                      A,
                      B] =
    new LoadTaskInput[(ArgA, ArgB),
                      (A, B)] {
      type SourceType = A

      def argsToTuple(sourceValue: SourceType, config: ConfigMap, inArgs: (ArgA, ArgB))
        : (SourceType, B) = (sourceValue, config.map(inArgs._2).asInstanceOf[B])
      def inputConfig(inArgs: (ArgA, ArgB)) = Set[ConfigValue[_]](inArgs._2)
      def getSource(inArgs: (ArgA, ArgB)) = inArgs._1
    }

  implicit def input3[ArgA <: Mappable[A],
                      ArgB <: ConfigValue[B],
                      ArgC <: ConfigValue[C],
                      A,
                      B,
                      C] =
    new LoadTaskInput[(ArgA, ArgB, ArgC),
                      (A, B, C)] {
      type SourceType = A

      def argsToTuple(sourceValue: SourceType, config: ConfigMap, inArgs: (ArgA, ArgB, ArgC))
        : (SourceType, B, C) = (sourceValue,
                                config.map(inArgs._2).asInstanceOf[B],
                                config.map(inArgs._3).asInstanceOf[C])
      def inputConfig(inArgs: (ArgA, ArgB, ArgC)) = Set[ConfigValue[_]](inArgs._2, inArgs._3)
      def getSource(inArgs: (ArgA, ArgB, ArgC)) = inArgs._1
    }
}
