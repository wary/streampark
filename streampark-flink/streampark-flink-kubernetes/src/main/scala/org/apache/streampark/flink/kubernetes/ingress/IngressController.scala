/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.flink.kubernetes.ingress

import org.apache.streampark.common.util.Implicits._
import org.apache.streampark.common.util.Logger

import org.apache.flink.configuration.Configuration
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.KubernetesClient

object IngressController extends Logger {

  private[this] val VERSION_REGEXP = "(\\d+\\.\\d+)".r

  private lazy val ingressStrategyV1 = new IngressStrategyV1()

  private lazy val ingressStrategyV1Beta1 = new IngressStrategyV1beta1()

  def getClusterVersion(k8sClient: KubernetesClient): Double = {
    VERSION_REGEXP.findFirstIn(k8sClient.getVersion.getGitVersion).get.toDouble
  }

  def getIngressStrategy(k8sClient: KubernetesClient): IngressStrategy = {
    if (getClusterVersion(k8sClient) >= 1.19) {
      ingressStrategyV1
    } else {
      ingressStrategyV1Beta1
    }
  }

  def configureIngress(domainName: String, clusterId: String, nameSpace: String, flinkConfig: Configuration): String = {
    val k8sClient = FlinkKubeClientFactory.getInstance.createFabric8ioKubernetesClient(flinkConfig)
    k8sClient.using(k8sClient => {
      getIngressStrategy(k8sClient).configureIngress(domainName, clusterId, nameSpace, k8sClient)
    })
  }

  def deleteIngress(clusterId: String, nameSpace: String, flinkConfig: Configuration): Unit = {
    val k8sClient = FlinkKubeClientFactory.getInstance.createFabric8ioKubernetesClient(flinkConfig)
    k8sClient.using(k8sClient => {
      getIngressStrategy(k8sClient).deleteIngress(clusterId, nameSpace, k8sClient)
    })
  }

  def getIngressUrlAddress(nameSpace: String, clusterId: String, flinkConfig: Configuration): Option[String] = {
    val k8sClient = FlinkKubeClientFactory.getInstance.createFabric8ioKubernetesClient(flinkConfig)
    k8sClient.using(k8sClient => {
      getIngressStrategy(k8sClient).getIngressUrl(nameSpace, clusterId, k8sClient)
    })
  }

  def getIngressUrlAddressWithClient(nameSpace: String, clusterId: String, k8sClient: KubernetesClient): Option[String] = {
    getIngressStrategy(k8sClient).getIngressUrl(nameSpace, clusterId, k8sClient)
  }

  def prepareIngressTemplateFiles(buildWorkspace: String, ingressTemplates: String): String = {
    IngressStrategy.prepareIngressTemplateFiles(buildWorkspace, ingressTemplates)
  }
}
