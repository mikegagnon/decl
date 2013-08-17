package com.mikegagnon.decl

/**
 * This file contains classes which are useful for "compiling" shorthand syntax for tasks (compute
 * and load tasks) into actual Task objects.
 *
 * Specifically, these classes are used for handling the input arguments for tasks.
 */

/**
 * ArgRequired is a type class used to determine whether or not an input argument is required.
 *
 * Assuming ArgA is an Argument[V], then X can be either V, or Option[V]
 * If X is V, then required = true
 * If X is Option[V], then required = false
 */
case class ArgRequired[ArgA <: Argument, X](val required: Boolean)

object ArgRequired {
  implicit def argRequired[ArgA <: Argument, A <: ArgA#V] = ArgRequired[ArgA, A](true)
  implicit def argOptional[ArgA <: Argument, O <: Option[ArgA#V]] = ArgRequired[ArgA, O](false)
}

/**
 * An ArgMap instance maps argument objects (i.e. Feature and ConfigValue objects) to the values
 * associated with those arguments (which are taken from features and config)
 */
case class ArgMap(features: FeatureMap, config: ConfigMap) {

  /**
   * The types and semantics of this function are a little tricky.
   * assume V == Argument#V.
   * ValueType will either be V or Option[V], which is enforced by the ArgRequired type class.
   * iff required == True, then ValueType == V
   * iff required == False, then ValueType == Option[V]
   *
   * if this function returns None, then that signifies that the argument is missing and required
   */
  def apply[ValueType](arg: Argument, required: Boolean): Option[ValueType] = {
    val result = if (required) {
      arg match {
        case arg: FeatureBase[_] => features.get(arg)
        case arg: ConfigValue[_] => Some(config(arg))
      }
    } else {
      arg match {
        case arg: FeatureBase[_] => Some(features.get(arg))
        case arg: ConfigValue[_] => Some(Some(config(arg)))
      }
    }

    result.asInstanceOf[Option[ValueType]]
  }
}

/**
 * Input is a type class used to associate a tuple of input argument types (of type InArgs)
 * with a tuple of value-types associated with InArgs.
 *
 * This type class can be instantiated iff the __subtypes__ of InArgs match the types of InTypes.
 *
 * Example 1:
 * if InArgs == (Feature[Int], ConfigValue[String])
 * then:
 *   InTypes could be (Int, String)
 *      --- or ---
 *   InTypes could be (Int, Option[String])
 *      --- or ---
 *   InTypes could be (Option[Int], String)
 *      --- or ---
 *   InTypes could be (Option[Int], Option[String])
 *
 * Note that the ArgRequired type class is used so that the contituent types of InTypes
 * may be Option types.
 */
abstract class Input[InArgs, InTypes] extends Serializable {

  /**
   * extract the values for inArgs from argMap.
   * If there are any missing values (that are required) then return None.
   */
  def argsToTuple(argMap: ArgMap, inArgs: InArgs): Option[InTypes]

  // The subset of inArgs that are FeatureBase objects
  def inputFeatures(inArgs: InArgs): Set[FeatureBase[_]]

  // The subset of inArgs that are ConfigValue objects
  def inputConfig(inArgs: InArgs): Set[ConfigValue[_]]
}

object Input {

  def inputFeatures(args: Argument*): Set[FeatureBase[_]] =
    args
      .toSet
      .flatMap { arg: Argument =>
        arg match {
          case arg: FeatureBase[_] => Some(arg)
          case _ => None: Option[FeatureBase[_]]
        }
      }

  def inputConfig(args: Argument*): Set[ConfigValue[_]] =
    args
      .toSet
      .flatMap { arg: Argument =>
        arg match {
          case arg: ConfigValue[_] => Some(arg)
          case _ => None: Option[ConfigValue[_]]
        }
      }

  // If ArgA <: Argument[V], then A is either V or Option[V] (enforced by the ArgRequired typeclass)
  implicit def input1[ArgA <: Argument, A]
      (implicit argRequiredA: ArgRequired[ArgA, A]) =
    new Input[ArgA, A] {
      def argsToTuple(argMap: ArgMap, inArg: ArgA): Option[A] =
        argMap[A](inArg, argRequiredA.required)
      def inputFeatures(inArg: ArgA) =
        Input.inputFeatures(inArg)
      def inputConfig(inArg: ArgA) =
        Input.inputConfig(inArg)
    }

  implicit def input2[ArgA <: Argument, ArgB <: Argument, A, B]
      (implicit argRequiredA: ArgRequired[ArgA, A],
                argRequiredB: ArgRequired[ArgB, B]) =
    new Input[(ArgA, ArgB), (A, B)] {
      def argsToTuple(argMap: ArgMap, inArgs: (ArgA, ArgB)): Option[(A, B)] =
        (argMap[A](inArgs._1, argRequiredA.required),
         argMap[B](inArgs._2, argRequiredB.required)) match {
          case (Some(a), Some(b)) => Some((a, b))
          case _ => None
        }
      def inputFeatures(inArgs: (ArgA, ArgB)) = Input.inputFeatures(inArgs._1, inArgs._2)
      def inputConfig(inArgs: (ArgA, ArgB)) = Input.inputConfig(inArgs._1, inArgs._2)
    }

  implicit def input3[ArgA <: Argument, ArgB <: Argument, ArgC <: Argument, A, B, C]
      (implicit argRequiredA: ArgRequired[ArgA, A],
                argRequiredB: ArgRequired[ArgB, B],
                argRequiredC: ArgRequired[ArgC, C]) =
    new Input[(ArgA, ArgB, ArgC), (A, B, C)] {
      def argsToTuple(argMap: ArgMap, inArgs: (ArgA, ArgB, ArgC)): Option[(A, B, C)] =
        (argMap[A](inArgs._1, argRequiredA.required),
         argMap[B](inArgs._2, argRequiredB.required),
         argMap[C](inArgs._3, argRequiredC.required)) match {
          case (Some(a), Some(b), Some(c)) => Some((a, b, c))
          case _ => None
        }
      def inputFeatures(inArgs: (ArgA, ArgB, ArgC)) =
        Input.inputFeatures(inArgs._1, inArgs._2, inArgs._3)
      def inputConfig(inArgs: (ArgA, ArgB, ArgC)) =
        Input.inputConfig(inArgs._1, inArgs._2, inArgs._3)
    }
}
