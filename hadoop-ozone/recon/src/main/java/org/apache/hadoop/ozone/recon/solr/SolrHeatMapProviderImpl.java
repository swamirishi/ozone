/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.ozone.recon.solr;

import com.google.common.net.HostAndPort;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.recon.api.types.EntityMetaData;
import org.apache.hadoop.ozone.recon.heatmap.IHeatMapProvider;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static org.apache.hadoop.ozone.recon.solr.SolrConstants.OZONE_SOLR_ADDRESS_KEY;
import static org.apache.hadoop.ozone.recon.solr.SolrConstants.OZONE_SOLR_SERVER_PORT_DEFAULT;

/**
 * This class is to retrieve heatmap data in a specific format for processing.
 */
public class SolrHeatMapProviderImpl implements IHeatMapProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(SolrHeatMapProviderImpl.class);
  private static final int NO_PORT = -1;
  private OzoneConfiguration ozoneConfiguration;
  private ReconOMMetadataManager omMetadataManager;

  /**
   * Initializes the config variables and
   * other objects needed by HeatMapProvider.
   * @param ozoneConfig
   * @param omMetadataMgr
   * @param namespaceSummaryManager
   * @param ozoneStorageContainerManager
   * @throws Exception
   */
  @Override
  public void init(OzoneConfiguration ozoneConfig,
                   ReconOMMetadataManager omMetadataMgr,
                   ReconNamespaceSummaryManager namespaceSummaryManager,
                   OzoneStorageContainerManager ozoneStorageContainerManager) {
    this.ozoneConfiguration = ozoneConfig;
    this.omMetadataManager = omMetadataMgr;
  }

  /**
   * This method allows heatmap provider to implement fetching of access
   * metadata of entities (volumes/buckets/keys/files) to return data
   * in below desired format for generation of heatmap.
   * List of EntityMetaData objects. Sample EntityMetaData object:
   * entityMetaDataObj:
   * val = "hivevol1676574631/hiveencbuck1676574631/enc_path/hive_tpcds/
   * store_sales/store_sales.dat"
   * readAccessCount = 155074
   *
   * @param path path of entity (volume/bucket/key)
   * @param entityType type of entity (volume/bucket/key)
   * @param startDate the start date since when access metadata to be retrieved
   * @return the list of EntityMetaData objects
   * @throws Exception
   */
  @Override
  public List<EntityMetaData> retrieveData(String path, String entityType,
                                           String startDate) throws Exception {
    SolrUtil solrUtil = new SolrUtil(omMetadataManager, ozoneConfiguration);
    InetSocketAddress solrAddr = getAndValidateInetSocketAddress();
    SolrHttpClient solrHttpClient = SolrHttpClient.getInstance();
    return solrUtil.queryLogs(path, entityType, startDate, solrHttpClient,
        solrAddr);
  }

  @NotNull
  private InetSocketAddress getAndValidateInetSocketAddress() {
    InetSocketAddress solrAddr = getSolrAddress(ozoneConfiguration);
    if (null == solrAddr) {
      throw new ConfigurationException(String.format("For heatmap " +
              "feature Solr host and port configuration must be provided " +
              "for config key %s. Example format -> <Host>:<Port>",
          OZONE_SOLR_ADDRESS_KEY));
    }
    return solrAddr;
  }

  /**
   * Retrieve the socket addresses of Apache Solr Server.
   *
   * @return Apache Solr server address
   * @throws IllegalArgumentException If the configuration is invalid
   */
  public static InetSocketAddress getSolrAddress(ConfigurationSource conf) {
    String solrAddresses = conf.get(OZONE_SOLR_ADDRESS_KEY);
    LOG.info("Solr Live Host Addresses: {}", solrAddresses);
    if (StringUtils.isEmpty(solrAddresses)) {
      throw new IllegalArgumentException(String.format("For heatmap " +
              "feature Solr host and port configuration must be provided " +
              "for config key %s. Example format -> <Host>:<Port>",
          OZONE_SOLR_ADDRESS_KEY));
    }
    String[] splitAddresses = solrAddresses.split(",");
    if (solrAddresses.length() > 0) {
      String name = splitAddresses[0];
      if (StringUtils.isEmpty(name)) {
        return null;
      }
      Optional<String> hostname = getHostName(name);
      if (!hostname.isPresent()) {
        throw new IllegalArgumentException("Invalid hostname for Solr Server: "
            + name);
      }
      int port = OZONE_SOLR_SERVER_PORT_DEFAULT;
      try {
        port = getHostPort(name).orElse(OZONE_SOLR_SERVER_PORT_DEFAULT);
      } catch (Exception exp) {
        String[] hostPort = name.split(":");
        if (hostPort.length > 1) {
          // Below split is done to extract out port number from
          // SOLR HOST URL format in zookeeper node.
          // URL format: https://<host>:<port>_solr
          String[] portSplit = hostPort[1].split("_");
          port = portSplit.length > 1 ? Integer.parseInt(portSplit[0]) :
              OZONE_SOLR_SERVER_PORT_DEFAULT;
        }
      }
      return NetUtils.createSocketAddr(hostname.get(), port);
    }
    return null;
  }

  /**
   * Gets the hostname or Indicates that it is absent.
   * @param value host or host:port
   * @return hostname
   */
  public static Optional<String> getHostName(String value) {
    if ((value == null) || value.isEmpty()) {
      return Optional.empty();
    }
    String hostname = value.replaceAll("\\:[0-9]+$", "");
    if (hostname.length() == 0) {
      return Optional.empty();
    } else {
      return Optional.of(hostname);
    }
  }

  /**
   * Gets the port if there is one, returns empty {@code OptionalInt} otherwise.
   * @param value  String in host:port format.
   * @return Port
   */
  public static OptionalInt getHostPort(String value) {
    if ((value == null) || value.isEmpty()) {
      return OptionalInt.empty();
    }
    int port = HostAndPort.fromString(value).getPortOrDefault(NO_PORT);
    if (port == NO_PORT) {
      return OptionalInt.empty();
    } else {
      return OptionalInt.of(port);
    }
  }
}
