package com.mikegagnon.decl

abstract class ComputeTask extends Task {

  /**
   * returns a FeatureMap that may only contain features specified in outputFeatures
   * the compute function may only access keys from features that are specified in inputFeatures
   * the compute function may only access keys in env that are specified in inputConfig
   */
  def compute(features: FeatureMap, config: ConfigMap): FeatureMap

}
