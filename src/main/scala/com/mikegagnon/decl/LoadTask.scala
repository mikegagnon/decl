package com.mikegagnon.decl

import com.twitter.scalding.Mappable
import cascading.flow.FlowDef

import com.twitter.scalding.{Dsl, Mode, TDsl, TypedPipe}

object LoadTask {

  import TDsl._
  import Dsl._

  /**
   * InArgs is either:
   *   - a Mappable source
   *   - a tuple that begins with a Mappable source, followed by EnvironmentValue objects
   */
  def apply[InArgs, InTypes, OutFeatures, OutTypes]
      (inOut: (InArgs, OutFeatures))
      (fn: Function1[InTypes, OutTypes])
      (implicit
        input: LoadTaskInput[InArgs, InTypes],
        output: Output[OutFeatures, OutTypes]) =
    new LoadTask {
      val (inArgs, outFeatures) = inOut

      override val inputConfig = input.inputConfig(inArgs)
      override val outputFeatures = output.outputFeatures(outFeatures)

      /**
       * HACK alert: here I use the fields API and then convert to a typed pipe.
       * For some reason, if I use the typed API directly then the TaskSetBuilder Load method
       * will fail at runtime when input.SourceType == Tuple2[...]
       */
      override def load(config: ConfigMap)(implicit flowDef: FlowDef, mode: Mode) =
        input
          .getSource(inArgs)
          .mapTo('featureMap) { sourceValue =>
            val inValues = input.argsToTuple(sourceValue, config, inArgs)
            val outValues: OutTypes = fn(inValues)
            output.toFeatureMap(outFeatures, outValues)
          }
          .toTypedPipe[FeatureMap]('featureMap)
    }
}

abstract class LoadTask extends Task {

  override final val inputFeatures: Set[FeatureBase[_]] = Set()

  /**
   * returns a TypedPipe[FeatureMap] with the following constraint:
   *    - the featureMaps may only contain features specified in outputFeatures
   * the load function may only access keys in config that are specified in inputConfig
   */
  def load(config: ConfigMap)(implicit flowDef: FlowDef, mode: Mode): TypedPipe[FeatureMap]

  final def run[V](groupByFeature: Feature[V], config: ConfigMap)
      (implicit flowDef: FlowDef, mode: Mode): TypedPipe[FeatureMap] = {

    if (!outputFeatures.contains(groupByFeature)) {
      throw new IllegalArgumentException(("You cannot run this LoadTask because it does not " +
        "output the groupBy feature. groupBy feature == %s, outputFeatures = %s")
        .format(groupByFeature, outputFeatures))
    }

    load(config)
  }

}
