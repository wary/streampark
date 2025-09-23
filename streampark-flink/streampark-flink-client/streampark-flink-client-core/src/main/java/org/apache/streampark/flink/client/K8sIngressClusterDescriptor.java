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

import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.artifact.KubernetesArtifactUploader;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.extensions.Ingress;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.rpc.AddressResolution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Kubernetes specific {@link ClusterDescriptor} implementation.
 */
public class K8sIngressClusterDescriptor extends KubernetesClusterDescriptor {

    private static final Logger LOG = LoggerFactory.getLogger(K8sIngressClusterDescriptor.class);

    private final Configuration flinkConfig;

    private final String clusterId;

    private final KubernetesClient kubernetesClient;

    public K8sIngressClusterDescriptor(
                                       Configuration flinkConfig,
                                       FlinkKubeClientFactory clientFactory,
                                       KubernetesArtifactUploader artifactUploader) {
        super(flinkConfig, clientFactory, artifactUploader);
        this.flinkConfig = flinkConfig;
        this.clusterId =
            checkNotNull(
                flinkConfig.get(KubernetesConfigOptions.CLUSTER_ID),
                "ClusterId must be specified!");
        this.kubernetesClient = K8sIngressClusterHelper.createK8sClient(flinkConfig);
    }

    private ClusterClientProvider<String> createClusterClientProvider(String clusterId) {
        return () -> {
            final Configuration configuration = new Configuration(flinkConfig);
            Ingress ingress = K8sIngressClusterHelper.getIngress(clusterId, this.kubernetesClient);
            if (ingress != null) {
                configuration.set(RestOptions.ADDRESS, ingress.getSpec().getRules().get(0).getHost());
                configuration.set(RestOptions.PORT, 80);
            } else {
                throw new RuntimeException(
                    new ClusterRetrieveException("Could not get the rest endpoint of " + clusterId));
            }

            try {
                // Flink client will always use Kubernetes service to contact with jobmanager. So we
                // have a pre-configured web monitor address. Using StandaloneClientHAServices to
                // create RestClusterClient is reasonable.
                return new RestClusterClient<>(
                    configuration,
                    clusterId,
                    (effectiveConfiguration, fatalErrorHandler) -> new StandaloneClientHAServices(
                        getWebMonitorAddress(effectiveConfiguration)));
            } catch (Exception e) {
                throw new RuntimeException(
                    new ClusterRetrieveException("Could not create the RestClusterClient.", e));
            }
        };
    }

    private String getWebMonitorAddress(Configuration configuration) throws Exception {
        Ingress ingress = K8sIngressClusterHelper.getIngress(clusterId, this.kubernetesClient);
        if (ingress != null) {
            return K8sIngressClusterHelper.getWebMonitorAddress(ingress, configuration);
        } else {
            AddressResolution resolution = AddressResolution.TRY_ADDRESS_RESOLUTION;
            final KubernetesConfigOptions.ServiceExposedType serviceType =
                configuration.get(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE);
            if (serviceType.isClusterIP()) {
                resolution = AddressResolution.NO_ADDRESS_RESOLUTION;
                LOG.warn(
                    "Please note that Flink client operations(e.g. cancel, list, stop,"
                        + " savepoint, etc.) won't work from outside the Kubernetes cluster"
                        + " since '{}' has been set to {}.",
                    KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE.key(),
                    serviceType);
            }
            return HighAvailabilityServicesUtils.getWebMonitorAddress(configuration, resolution);
        }
    }

    @Override
    public ClusterClientProvider<String> retrieve(String clusterId) {
        final ClusterClientProvider<String> clusterClientProvider =
            createClusterClientProvider(clusterId);

        try (ClusterClient<String> clusterClient = clusterClientProvider.getClusterClient()) {
            LOG.info(
                "Retrieve flink cluster {} successfully, JobManager Web Interface: {}",
                clusterId,
                clusterClient.getWebInterfaceURL());
        }
        return clusterClientProvider;
    }

    @Override
    public void close() {
        super.close();
        try {
            kubernetesClient.close();
        } catch (Exception e) {
            LOG.error("failed to close k8s client, exception {}", e.toString());
        }
    }
}
