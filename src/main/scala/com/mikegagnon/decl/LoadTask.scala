package com.mikegagnon.decl

import com.twitter.scalding.Mappable
import cascading.flow.FlowDef

import com.twitter.scalding.{Mode, TypedPipe}

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
