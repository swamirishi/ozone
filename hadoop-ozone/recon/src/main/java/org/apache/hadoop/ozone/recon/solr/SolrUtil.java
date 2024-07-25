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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.api.types.EntityMetaData;
import org.apache.hadoop.ozone.recon.api.types.HealthCheckResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.solr.http.HttpRequestWrapper;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.recon.solr.SolrConstants.OZONE_RECON_SOLR_TIMEZONE_KEY;

/**
 * This class is general utility class for handling
 * Solr query functions.
 */
public class SolrUtil {
  private static final Logger LOG =
      LoggerFactory.getLogger(SolrUtil.class);
  public static final String DEFAULT_TIMEZONE_VALUE = "UTC";

  private OzoneConfiguration ozoneConfiguration;
  private final ReconOMMetadataManager omMetadataManager;
  private SimpleDateFormat dateFormat = new SimpleDateFormat(
      "yyyy-MM-dd'T'HH:mm:ss'Z'");
  private final String timeZone;

  @Inject
  public SolrUtil(ReconOMMetadataManager omMetadataManager,
                  OzoneConfiguration ozoneConfiguration) {
    this.omMetadataManager = omMetadataManager;
    this.ozoneConfiguration = ozoneConfiguration;
    this.timeZone = this.ozoneConfiguration.get(OZONE_RECON_SOLR_TIMEZONE_KEY,
        DEFAULT_TIMEZONE_VALUE);
    if (timeZone != null) {
      LOG.info("Setting timezone to " + timeZone);
      try {
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
      } catch (Throwable t) {
        LOG.error("Error setting timezone. TimeZone = " + timeZone);
      }
    }
  }

  public List<EntityMetaData>
      queryLogs(String path, String entityType, String startDate,
            SolrHttpClient solrHttpClient, InetSocketAddress solrAddress) {
    try {
      return SecurityUtil.doAsCurrentUser(
          (PrivilegedExceptionAction<List<EntityMetaData>>) () -> {
            List<NameValuePair> urlParameters = new ArrayList<>();
            validateAndAddSolrReqParam(escapeQueryParamVal(path),
                "resource", urlParameters);
            validateAndAddSolrReqParam(entityType, "resType", urlParameters);
            validateStartDate(startDate, urlParameters);
            final String solrAuditResp =
                solrHttpClient.sendRequest(
                    prepareHttpRequest(urlParameters, solrAddress,
                        "/solr/ranger_audits/query"));
            LOG.info("Solr Response: {}", solrAuditResp);
            JsonElement jsonElement = JsonParser.parseString(solrAuditResp);
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            JsonElement facets = jsonObject.get("facets");
            JsonElement resources = facets.getAsJsonObject().get("resources");
            JsonObject facetsBucketsObject = new JsonObject();
            if (null != resources) {
              facetsBucketsObject = resources.getAsJsonObject();
            }
            ObjectMapper objectMapper = new ObjectMapper();

            AuditLogFacetsResources auditLogFacetsResources =
                objectMapper.readValue(
                    facetsBucketsObject.toString(),
                    AuditLogFacetsResources.class);
            EntityMetaData[] metaDataList =
                auditLogFacetsResources.getMetaDataList();
            if (null != metaDataList && metaDataList.length > 0) {
              return Arrays.stream(metaDataList).collect(Collectors.toList());
            }
            return Collections.emptyList();
          });
    } catch (JsonProcessingException e) {
      LOG.error("Solr Query Output Processing Error: {} ", e);
    } catch (IOException e) {
      LOG.error("Error while generating the access heatmap: {} ", e);
    }
    return null;
  }

  private String escapeQueryParamVal(String path) {
    StringBuilder sb = new StringBuilder();
    if (!StringUtils.isEmpty(path)) {
      sb.append(ClientUtils.escapeQueryChars(path));
      sb.append("/");
      sb.append("*");
    }
    return sb.toString();
  }

  private void validateStartDate(String startDate,
                                 List<NameValuePair> urlParameters) {
    if (!StringUtils.isEmpty(startDate)) {
      ZonedDateTime lastXUnitsOfZonedDateTime = null;
      if (null == LastXUnit.getType(startDate)) {
        startDate = validateStartDate(startDate);
      }
      if (null != LastXUnit.getType(startDate)) {
        lastXUnitsOfZonedDateTime =
            lastXUnitsOfTime(LastXUnit.getType(startDate));
      } else {
        lastXUnitsOfZonedDateTime =
            epochMilliSToZDT(startDate);
      }
      urlParameters.add(new BasicNameValuePair("fq",
          setDateRange("evtTime",
              Date.from(lastXUnitsOfZonedDateTime.toInstant()), null)));
    }
  }

  private void validateAndAddSolrReqParam(
      String paramVal, String paramName,
      List<NameValuePair> urlParameters) {
    if (!StringUtils.isEmpty(paramVal)) {
      StringBuilder sb = new StringBuilder(paramName);
      sb.append(":");
      sb.append(paramVal);
      urlParameters.add(new BasicNameValuePair("fq", sb.toString()));
    }
  }

  private HttpRequestWrapper preparePingHttpRequest(List<NameValuePair> urlParameters,
                                                    InetSocketAddress solrAddress,
                                                    String uri) {
    // add request parameter, form parameters
    urlParameters.add(new BasicNameValuePair("q", "*:*"));
    urlParameters.add(new BasicNameValuePair("wt", "json"));
    urlParameters.add(new BasicNameValuePair("fl",
        "access, agent, repo, resource, resType, event_count"));
    urlParameters.add(new BasicNameValuePair("fq", "access:read"));
    urlParameters.add(new BasicNameValuePair("fq", "repo:cm_ozone"));
    urlParameters.add(new BasicNameValuePair("rows", "0"));

    HttpRequestWrapper requestWrapper =
        new HttpRequestWrapper(solrAddress.getHostName(),
            solrAddress.getPort(), uri,
            urlParameters, HttpRequestWrapper.HttpReqType.POST);
    return requestWrapper;
  }

  private HttpRequestWrapper prepareHttpRequest(
      List<NameValuePair> urlParameters, InetSocketAddress solrAddress,
      String uri) {
    // add request parameter, form parameters
    urlParameters.add(new BasicNameValuePair("q", "*:*"));
    urlParameters.add(new BasicNameValuePair("wt", "json"));
    urlParameters.add(new BasicNameValuePair("fl",
        "access, agent, repo, resource, resType, event_count"));
    urlParameters.add(new BasicNameValuePair("fq", "access:read"));
    urlParameters.add(new BasicNameValuePair("fq", "repo:cm_ozone"));

    urlParameters.add(new BasicNameValuePair("sort", "event_count desc"));
    urlParameters.add(new BasicNameValuePair("start", "0"));
    urlParameters.add(new BasicNameValuePair("rows", "0"));

    urlParameters.add(new BasicNameValuePair("json.facet", "{\n" +
        "    resources:{\n" +
        "      type : terms,\n" +
        "      field : resource,\n" +
        "      sort : \"read_access_count desc\",\n" +
        "      limit : 100,\n" +
        "      facet:{\n" +
        "        read_access_count : \"sum(event_count)\"\n" +
        "      }\n" +
        "    }\n" +
        "  }"));
    HttpRequestWrapper requestWrapper =
        new HttpRequestWrapper(solrAddress.getHostName(),
            solrAddress.getPort(), uri,
            urlParameters, HttpRequestWrapper.HttpReqType.POST);
    return requestWrapper;
  }

  public String setDateRange(String fieldName, Date fromDate, Date toDate) {
    String fromStr = "*";
    String toStr = "NOW";
    if (fromDate != null) {
      fromStr = dateFormat.format(fromDate);
    }
    if (toDate != null) {
      toStr = dateFormat.format(toDate);
    }
    return fieldName + ":[" + fromStr + " TO " + toStr + "]";
  }

  private String validateStartDate(String startDate) {
    long epochMilliSeconds = 0L;
    try {
      epochMilliSeconds = Long.parseLong(startDate);
    } catch (NumberFormatException nfe) {
      LOG.error(
          "Unsupported Last X units of time : {}, falling back to default 24H",
          startDate);
      return LastXUnit.TWENTY_FOUR_HOUR.getValue();
    }
    if (epochMilliSeconds > Instant.now().toEpochMilli()) {
      LOG.error(
          "Unsupported Last X units of time : {}, falling back to default 24H",
          startDate);
      return LastXUnit.TWENTY_FOUR_HOUR.getValue();
    }
    return startDate;
  }

  private ZonedDateTime epochMilliSToZDT(String epochMilliSeconds) {
    Long lEpochMilliSeconds = Long.parseLong(epochMilliSeconds);
    return ZonedDateTime.ofInstant(
        Instant.ofEpochMilli(lEpochMilliSeconds),
        TimeZone.getTimeZone(timeZone).toZoneId());
  }

  private ZonedDateTime lastXUnitsOfTime(LastXUnit lastXUnit) {
    ZonedDateTime zonedDateTime = null;
    switch (lastXUnit) {
    case TWENTY_FOUR_HOUR:
      zonedDateTime = Instant.now().atZone(
              TimeZone.getTimeZone(timeZone).toZoneId())
          .minus(24, ChronoUnit.HOURS);
      break;
    case SEVEN_DAYS:
      zonedDateTime = Instant.now().atZone(
          TimeZone.getTimeZone(timeZone).toZoneId()).minus(7, ChronoUnit.DAYS);
      break;
    case NINETY_DAYS:
      zonedDateTime = Instant.now().atZone(
              TimeZone.getTimeZone(timeZone).toZoneId())
          .minus(90, ChronoUnit.DAYS);
      break;
    default:
      throw new IllegalArgumentException(
          "Unsupported Last X units of time : " + lastXUnit);
    }
    return zonedDateTime;
  }

  public HealthCheckResponse doSolrHealthCheck(SolrHttpClient solrHttpClient, InetSocketAddress solrAddress) {
    try {
      return SecurityUtil.doAsCurrentUser(
          (PrivilegedExceptionAction<HealthCheckResponse>) () -> {
            List<NameValuePair> urlParameters = new ArrayList<>();

            final String solrPingResp =
                solrHttpClient.sendRequest(
                    preparePingHttpRequest(urlParameters, solrAddress, "/solr/ranger_audits/query"));
            LOG.info("Solr Ping Response: {}", solrPingResp);
            if (solrPingResp.isEmpty()) {
              return new HealthCheckResponse.Builder("UnHealthy",
                  Response.Status.SERVICE_UNAVAILABLE.getStatusCode()).build();
            }
            return new HealthCheckResponse.Builder("Healthy", Response.Status.OK.getStatusCode()).build();
          });
    } catch (JsonProcessingException e) {
      LOG.error("Solr Query Output Processing Error: {} ", e);
      return new HealthCheckResponse.Builder("UnHealthy",
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()).build();
    } catch (IOException e) {
      LOG.error("Error while generating the access heatmap: {} ", e);
      return new HealthCheckResponse.Builder("UnHealthy",
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()).build();
    }
  }
}
