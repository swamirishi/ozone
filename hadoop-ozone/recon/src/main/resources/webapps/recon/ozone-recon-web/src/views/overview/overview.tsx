/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React from 'react';
import {Row, Col, Icon, Tooltip} from 'antd';
import OverviewCard from 'components/overviewCard/overviewCard';
import axios from 'axios';
import {IStorageReport} from 'types/datanode.types';
import moment from 'moment';
import AutoReloadPanel from 'components/autoReloadPanel/autoReloadPanel';
import {showDataFetchError,byteToSize} from 'utils/common';
import {AutoReloadHelper} from 'utils/autoReloadHelper';
import filesize from 'filesize';
import './overview.less';

const size = filesize.partial({round: 1});

interface IClusterStateResponse {
  missingContainers: number;
  totalDatanodes: number;
  healthyDatanodes: number;
  pipelines: number;
  storageReport: IStorageReport;
  containers: number;
  volumes: number;
  buckets: number;
  keys: number;
  openContainers: number;
  deletedContainers: number;
  keysPendingDeletion: number;
}

interface IOverviewState {
  loading: boolean;
  datanodes: string;
  pipelines: number;
  storageReport: IStorageReport;
  containers: number;
  volumes: number;
  buckets: number;
  keys: number;
  missingContainersCount: number;
  lastRefreshed: number;
  lastUpdatedOMDBDelta: number;
  lastUpdatedOMDBFull: number;
  omStatus: string;
  openContainers: number;
  deletedContainers: number;
  keysPendingDeletion: number;
  openSummarytotalUnrepSize: number,
  openSummarytotalRepSize: number,
  openSummarytotalOpenKeys: number,
  deletePendingSummarytotalUnrepSize: number,
  deletePendingSummarytotalRepSize: number,
  deletePendingSummarytotalDeletedKeys: number,
}

export class Overview extends React.Component<Record<string, object>, IOverviewState> {
  interval = 0;
  autoReload: AutoReloadHelper;

  constructor(props = {}) {
    super(props);
    this.state = {
      loading: false,
      datanodes: '',
      pipelines: 0,
      storageReport: {
        capacity: 0,
        used: 0,
        remaining: 0
      },
      containers: 0,
      volumes: 0,
      buckets: 0,
      keys: 0,
      missingContainersCount: 0,
      lastRefreshed: 0,
      lastUpdatedOMDBDelta: 0,
      lastUpdatedOMDBFull: 0,
      omStatus: '',
      openContainers: 0,
      deletedContainers: 0,
      keysPendingDeletion: 0,
      openSummarytotalUnrepSize: 0,
      openSummarytotalRepSize: 0,
      openSummarytotalOpenKeys: 0,
      deletePendingSummarytotalUnrepSize: 0,
      deletePendingSummarytotalRepSize: 0,
      deletePendingSummarytotalDeletedKeys: 0,
    };
    this.autoReload = new AutoReloadHelper(this._loadData);
  }

  _loadData = () => {
    this.setState({
      loading: true
    });
    Promise.allSettled([
      axios.get('/api/v1/clusterState'),
      axios.get('/api/v1/task/status'),
      axios.get('/api/v1/keys/open?limit=0'),
      axios.get('/api/v1/keys/deletePending?limit=1'),
      axios.get('/api/v1/heatmap/healthCheck')
    ]).then(axios.spread((clusterStateResponse, taskstatusResponse, openResponse, deletePendingResponse, healthCheckResponse) => {
      
      const clusterState: IClusterStateResponse = clusterStateResponse && clusterStateResponse.value && clusterStateResponse.value.data;
      const taskStatus = taskstatusResponse && taskstatusResponse.value && taskstatusResponse.value.data;
      const omDBDeltaObject = taskStatus && taskStatus.find((item:any) => item.taskName === 'OmDeltaRequest');
      const omDBFullObject = taskStatus && taskStatus.find((item: any) => item.taskName === 'OmSnapshotRequest');
      const openKeysSummary = openResponse && openResponse.value && openResponse.value.data && openResponse.value.data.keysSummary;
      const deletePendingSummary = deletePendingResponse && deletePendingResponse.value && deletePendingResponse.value.data && deletePendingResponse.value.data.keysSummary;
      const healthcheckStatus = healthCheckResponse && healthCheckResponse.value && healthCheckResponse.value.data && healthCheckResponse.value.data.message;
      const clusterNotEmptyCheck = clusterState !== undefined && Object.keys(clusterState).length !== 0;
      sessionStorage.setItem('heatmapHealthCheck', JSON.stringify(healthcheckStatus === 'Healthy' ? true : false));
      this.setState({
        loading: false,
        datanodes: clusterNotEmptyCheck  ? `${clusterState.healthyDatanodes}/${clusterState.totalDatanodes}` : '0',
        storageReport: clusterNotEmptyCheck ? clusterState.storageReport : {capacity: 0, used: 0, remaining: 0},
        pipelines: clusterNotEmptyCheck ? clusterState.pipelines : 0,
        containers: clusterNotEmptyCheck  ? clusterState.containers : 0,
        volumes: clusterNotEmptyCheck  ? clusterState.volumes : 0,
        buckets: clusterNotEmptyCheck ? clusterState.buckets :0,
        keys: clusterNotEmptyCheck ? clusterState.keys : 0,
        missingContainersCount : clusterNotEmptyCheck ? clusterState.missingContainers : 0,
        openContainers: clusterNotEmptyCheck ? clusterState.openContainers: 0,
        keysPendingDeletion: clusterNotEmptyCheck ? clusterState.keysPendingDeletion: 0,
        deletedContainers: clusterNotEmptyCheck ? clusterState.deletedContainers: 0,
        lastRefreshed: Number(moment()),
        lastUpdatedOMDBDelta: omDBDeltaObject && omDBDeltaObject.lastUpdatedTimestamp,
        lastUpdatedOMDBFull: omDBFullObject && omDBFullObject.lastUpdatedTimestamp,
        openSummarytotalUnrepSize: openKeysSummary && openKeysSummary.totalUnreplicatedDataSize,
        openSummarytotalRepSize: openKeysSummary && openKeysSummary.totalReplicatedDataSize,
        openSummarytotalOpenKeys: openKeysSummary && openKeysSummary.totalOpenKeys,
        deletePendingSummarytotalUnrepSize: deletePendingSummary && deletePendingSummary.totalUnreplicatedDataSize,
        deletePendingSummarytotalRepSize: deletePendingSummary && deletePendingSummary.totalReplicatedDataSize,
        deletePendingSummarytotalDeletedKeys: deletePendingSummary && deletePendingSummary.totalDeletedKeys,
      });
    })).catch(error => {
      this.setState({
        loading: false
      });
      showDataFetchError(error.toString());
    });
  };


  omSyncData = () => {
    this.setState({
      loading: true
    });

    axios.get('/api/v1/triggerdbsync/om').then( omstatusResponse => {    
      const omStatus = omstatusResponse.data;
      this.setState({
        loading: false,
        omStatus: omStatus
      });
    }).catch(error => {
      this.setState({
        loading: false
      });
      showDataFetchError(error.toString());
    });
  };

  componentDidMount(): void {
    this._loadData();
    this.autoReload.startPolling();
  }

  componentWillUnmount(): void {
    this.autoReload.stopPolling();
  }

  render() {
    const {loading, datanodes, pipelines, storageReport, containers, volumes, buckets, openSummarytotalUnrepSize, openSummarytotalRepSize, openSummarytotalOpenKeys,
      deletePendingSummarytotalUnrepSize,deletePendingSummarytotalRepSize,deletePendingSummarytotalDeletedKeys,keysPendingDeletion,
      keys, missingContainersCount, lastRefreshed, lastUpdatedOMDBDelta, lastUpdatedOMDBFull, omStatus, openContainers, deletedContainers } = this.state;
      
    const datanodesElement = (
      <span>
        <Icon type='check-circle' theme='filled' className='icon-success icon-small'/> {datanodes} <span className='ant-card-meta-description meta'>HEALTHY</span>
      </span>
    );
    const openSummaryData = (
        <div>
          {openSummarytotalRepSize!== undefined ? byteToSize(openSummarytotalRepSize, 1): '0'}   <span className='ant-card-meta-description meta'>Total Replicated Data Size</span><br />
          {openSummarytotalUnrepSize!== undefined ? byteToSize(openSummarytotalUnrepSize, 1): '0'}  <span className='ant-card-meta-description meta'>Total UnReplicated Data Size</span><br />
          {openSummarytotalOpenKeys !== undefined ? openSummarytotalOpenKeys: '0'}  <span className='ant-card-meta-description meta'>Total Open Keys</span>
        </div>
    );
    const deletePendingSummaryData = (
      <div>
        {deletePendingSummarytotalRepSize!== undefined ? byteToSize(deletePendingSummarytotalRepSize, 1): '0'}  <span className='ant-card-meta-description meta'>Total Replicated Data Size</span><br />
        {deletePendingSummarytotalUnrepSize!== undefined ? byteToSize(deletePendingSummarytotalUnrepSize,1): '0'}  <span className='ant-card-meta-description meta'>Total UnReplicated Data Size</span><br />
        {deletePendingSummarytotalDeletedKeys !== undefined ? deletePendingSummarytotalDeletedKeys: '0'}  <span className='ant-card-meta-description meta'>Total Pending Delete Keys</span>
      </div>
  );
    const containersTooltip = missingContainersCount === 1 ? 'container is missing' : 'containers are missing';
    const containersLink = missingContainersCount > 0 ? '/MissingContainers' : '/Containers';
    const duLink = '/DiskUsage';
    const containersElement = missingContainersCount > 0 ? (
      <span>
        <Tooltip placement='bottom' title={missingContainersCount > 1000 ? `1000+ Containers are missing. For more information, go to the Containers page.` : `${missingContainersCount} ${containersTooltip}`}>
          <Icon type='exclamation-circle' theme='filled' className='icon-failure icon-small'/>
        </Tooltip>
        <span className='padded-text'>{containers - missingContainersCount}/{containers}</span>
      </span>
    ) :
      <div>
          <span>{containers.toString()}   </span>
        <Tooltip placement='bottom' title='Number of open containers'>
          <span>({openContainers})</span>
        </Tooltip>
      </div>
    const clusterCapacity = storageReport && `${size(storageReport.capacity - storageReport.remaining)}/${size(storageReport.capacity)}`;
    return (
      <div className='overview-content'>
        <div className='page-header'>
          Overview
          <AutoReloadPanel isLoading={loading} lastRefreshed={lastRefreshed} 
          lastUpdatedOMDBDelta={lastUpdatedOMDBDelta} lastUpdatedOMDBFull={lastUpdatedOMDBFull}
          togglePolling={this.autoReload.handleAutoReloadToggle} onReload={this._loadData} omSyncLoad={this.omSyncData} omStatus={omStatus}/>
        </div>
        <Row gutter={[25, 25]}>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard
              hoverable loading={loading} title='Datanodes'
              data={datanodesElement} icon='cluster'
              linkToUrl='/Datanodes'/>
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard
              hoverable loading={loading} title='Pipelines'
              data={pipelines.toString()} icon='deployment-unit'
              linkToUrl='/Pipelines'/>
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard
              loading={loading} title='Cluster Capacity' data={clusterCapacity}
              icon='database'
              storageReport={storageReport}/>
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard
              loading={loading} title='Containers' data={containersElement}
              icon='container'
              error={missingContainersCount > 0} linkToUrl={containersLink}/>
          </Col>
        </Row>
        <Row gutter={[25, 25]}>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard loading={loading} title='Volumes' data={volumes.toString()} icon='inbox' linkToUrl={duLink}/>
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard loading={loading} title='Buckets' data={buckets.toString()} icon='folder-open' linkToUrl='/Buckets'/>
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard loading={loading} title='Keys' data={keys.toString()} icon='file-text'/>
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard loading={loading} title='Deleted Containers' data={deletedContainers.toString()} icon='delete' />
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard loading={loading} title='Pending Key Deletions' data={keysPendingDeletion.toString()} icon='delete' />
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6} className='summary-font'>
            <OverviewCard loading={loading} title='Open Keys Summary' data={openSummaryData} icon='file-text' />
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6} className='summary-font'>
            <OverviewCard loading={loading} title='Pending Deleted Keys Summary' data={deletePendingSummaryData} icon='delete' />
          </Col>
        </Row>
      </div>
    );
  }
}
