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

import org.apache.streampark.flink.kubernetes.PodTemplateTool;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ObjectMeta;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.StatusDetails;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.extensions.Ingress;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.hadoop.shaded.com.nimbusds.jose.util.IOUtils;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class K8sIngressClusterHelper {

    public static KubernetesClient createK8sClient(Configuration flinkConfig) {
        return FlinkKubeClientFactory.getInstance().createFabric8ioKubernetesClient(flinkConfig);
    }

    public static String getOrCreateIngress(String clusterId, KubernetesClient client,
                                            Configuration flinkConfig) throws IOException {
        String ingressTemplateFile = flinkConfig.getString(PodTemplateTool.KUBERNETES_INGRESS_TEMPLATE().key(), "");
        if (StringUtils.isNotBlank(ingressTemplateFile)) {
            String ingressTemplate = IOUtils.readFileToString(new File(ingressTemplateFile));
            Yaml yaml = new Yaml();
            Ingress ingress = yaml.loadAs(ingressTemplate, Ingress.class);
            ObjectMeta metaData = ingress.getMetadata() == null ? new ObjectMeta() : ingress.getMetadata();
            metaData.setName(clusterId);
            metaData.setNamespace(client.getNamespace());
            ingress.setMetadata(metaData);
            if (getIngress(clusterId, client) == null) {
                client.resource(ingress).create();
            } else {
                client.resource(ingress).update();
            }
            return getWebMonitorAddress(ingress, flinkConfig);
        }
        return "";
    }

    public static List<StatusDetails> deleteIngress(String clusterId, KubernetesClient client) {
        return client.network().ingresses().withName(clusterId).delete();
    }

    public static Ingress getIngress(String clusterId, KubernetesClient client) {
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
