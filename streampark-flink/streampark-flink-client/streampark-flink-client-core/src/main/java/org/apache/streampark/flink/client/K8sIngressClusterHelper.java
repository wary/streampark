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

package org.apache.streampark.flink.client;

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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.IntOrString;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ObjectMeta;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.StatusDetails;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.extensions.HTTPIngressPath;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.extensions.HTTPIngressRuleValue;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.extensions.Ingress;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.extensions.IngressBackend;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.extensions.IngressRule;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.extensions.IngressSpec;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.KubernetesClient;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class K8sIngressClusterHelper {

    public static String KUBERNETES_INGRESS_PROXY = "kubernetes.ingress.proxy";

    public static KubernetesClient createK8sClient(Configuration flinkConfig) {
        return FlinkKubeClientFactory.getInstance().createFabric8ioKubernetesClient(flinkConfig);
    }

    public static String getOrCreateIngress(String clusterId, KubernetesClient client, Configuration flinkConfig) {
        Ingress ingress = getIngress(clusterId, client, flinkConfig);
        if (ingress == null) {
            String proxy =
                flinkConfig.getString(KUBERNETES_INGRESS_PROXY, "").replaceAll("\\{clusterId\\}", clusterId);
            if (StringUtils.isNotBlank(proxy)) {
                ingress = new Ingress();
                ObjectMeta metaData = new ObjectMeta();
                metaData.setName(clusterId);
                ingress.setMetadata(metaData);
                IngressSpec spec = new IngressSpec();

                List<IngressRule> ingressRules = Arrays.stream(proxy.split(","))
                    .map(host -> {
                        HTTPIngressPath ingressPath = new HTTPIngressPath();
                        ingressPath.setBackend(new IngressBackend(null, clusterId + "-rest", new IntOrString(8081)));
                        ingressPath.setPath("/");
                        ingressPath.setPathType("ImplementationSpecific");

                        HTTPIngressRuleValue httpIngressRuleValue = new HTTPIngressRuleValue();
                        httpIngressRuleValue.setPaths(Arrays.asList(ingressPath));

                        IngressRule ingressRule = new IngressRule();
                        ingressRule.setHost(host);
                        ingressRule.setHttp(httpIngressRuleValue);

                        return ingressRule;
                    })
                    .collect(Collectors.toList());

                spec.setRules(ingressRules);
                ingress.setSpec(spec);
                client.resource(ingress).create();
            }
        }
        return getWebMonitorAddress(ingress, flinkConfig);
    }

    public static List<StatusDetails> deleteIngress(String clusterId, KubernetesClient client,
                                                    Configuration flinkConfig) {
        Ingress ingress = new Ingress();
        ObjectMeta metaData = new ObjectMeta();
        metaData.setName(clusterId);
        ingress.setMetadata(metaData);
        return client.resource(ingress).delete().stream().collect(Collectors.toList());
    }

    public static Ingress getIngress(String clusterId, KubernetesClient client, Configuration flinkConfig) {
        Ingress ingress = new Ingress();
        ObjectMeta metaData = new ObjectMeta();
        metaData.setName(clusterId);
        ingress.setMetadata(metaData);
        return client.resource(ingress).get();
    }

    public static String getWebMonitorAddress(Ingress ingress, Configuration flinkConfig) {
        if (ingress != null) {
            String http = "http://";
            if (SecurityOptions.isRestSSLEnabled(flinkConfig)) {
                http = "https://";
            }
            return http + ingress.getSpec().getRules().get(0).getHost() + ":80";
        } else {
            return null;
        }
    }
}
