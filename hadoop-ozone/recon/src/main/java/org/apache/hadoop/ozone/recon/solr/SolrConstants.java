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

/**
 * This class contains constants for Solr related configuration keys used in
 * Recon for heatmap.
 */
public final class SolrConstants {

  /**
   * Never constructed.
   */
  private SolrConstants() {
  }

  public static final String OZONE_SOLR_ADDRESS_KEY = "solr.address";
  public static final int OZONE_SOLR_SERVER_PORT_DEFAULT = 8995;
  public static final String OZONE_RECON_SOLR_TIMEZONE_KEY =
      "ozone.recon.solr.timezone";

}
