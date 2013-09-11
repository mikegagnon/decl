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

object ComputeTask {

  def apply[InArgs, InTypes, OutFeatures, OutTypes]
      (inOut: (InArgs, OutFeatures))
      (fn: Function1[InTypes, OutTypes])
      (implicit
       input: Input[InArgs, InTypes],
       output: Output[OutFeatures, OutTypes]) =
    new ComputeTask {
      val (inArgs, outFeatures) = inOut

      override val inputConfig = input.inputConfig(inArgs)
      override val inputFeatures = input.inputFeatures(inArgs)
      override val outputFeatures = output.outputFeatures(outFeatures)

      override def compute(features: FeatureMap, config: ConfigMap): FeatureMap = {
        // inValues is None when a required feature is missing
        val inValues: Option[InTypes] = input.argsToTuple(ArgMap(features, config), inArgs)

        inValues match {
          case None => FeatureMap()
          case Some(inValues) => output.toFeatureMap(outFeatures, fn(inValues))
        }
      }
    }
}

abstract class ComputeTask extends Task {

  /**
   * returns a FeatureMap that may only contain features specified in outputFeatures
   * the compute function may only access keys from features that are specified in inputFeatures
   * the compute function may only access keys in env that are specified in inputConfig
   */
  def compute(features: FeatureMap, config: ConfigMap): FeatureMap

}
