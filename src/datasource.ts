/*
 *
 *  Copyright (C) 2019 Bolt Analytics Corporation
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 *
 */

import { DataQueryRequest, DataSourceApi, DataSourceInstanceSettings } from '@grafana/ui';
import { DataFrame } from '@grafana/data';
import { BoltQuery, BoltOptions } from './types';
import { getBackendSrv } from '@grafana/runtime';
import { Utils } from 'datasourceUtils';

import _ from 'lodash';

export class BoltDatasource extends DataSourceApi<BoltQuery, BoltOptions> {
  data: DataFrame[] = [];
  baseUrl: any = '';
  anCollection = '';
  jobConfigCollection = '';
  rawCollection = '';
  rawCollectionType = 'single';
  timestampField = 'timestamp';
  anomalyThreshold = 5;
  topN = 10;
  rawCollectionWindow = 1;
  backendSrv: any;
  qTemp: any;
  $q: any;
  templateSrv: any;

  jobIdMappings: { dashboards: any; panels: any };

  totalCount?: number = undefined;

  facets: any = {};

  constructor(instanceSettings: DataSourceInstanceSettings<BoltOptions>, $q: any, templateSrv: any) {
    super(instanceSettings);

    this.jobIdMappings = { dashboards: {}, panels: {} };
    this.$q = $q;
    this.templateSrv = templateSrv;
    this.baseUrl = instanceSettings.url;

    if (instanceSettings.jsonData) {
    }

    this.backendSrv = getBackendSrv();
  }

  metricFindQuery(query: string) {
    return Promise.reject({
      status: 'error',
      message: 'Not supported yet',
      title: 'Error while adding ' + query,
    });
  }

  query(options: DataQueryRequest<BoltQuery>): any {
    const targetPromises = options.targets
      .map((query: BoltQuery) => {
        if (!query.parsingStream) {
          return Promise.reject([
            {
              status: 'error',
              message: 'Error',
              title: 'Error',
            },
          ]);
        }

        const streamStr: any = query.parsingStream.match(/CREATE STREAM (.*?) .*/i);
        let table: any;
        if (streamStr) {
          table = streamStr[1];
        }

        if (query.filteringStream) {
          const tableStr: any = query.filteringStream.match(/CREATE TABLE (.*?) .*/i);
          if (tableStr) {
            table = tableStr[1];
          }
        }

        // const ksqlQuery =
        //   '{"ksql": "select * from ' +
        //   table +
        //   " where WINDOWSTART >= '" +
        //   options.range.from.toISOString() +
        //   "' AND WINDOWEND <= '" +
        //   options.range.to.toISOString() +
        //   '\' EMIT CHANGES;","streamsProperties": {"auto.offset.reset":"earliest"}}';

        const q = {
          ksql:
            'select * from ' +
            table +
            " WHERE jobID = 'prod-gymboree-stage-nginx-access-ohio' AND WINDOWSTART >= " +
            options.range.from.valueOf() +
            ' AND WINDOWSTART <= ' +
            options.range.to.valueOf() +
            ';',
          streamsProperties: { 'auto.offset.reset': 'earliest' },
        };

        const httpOpts = {
          url: this.baseUrl + '/query',
          method: 'POST',
          headers: { Accept: 'application/json; charset=utf-8' },
          data: q,
        };

        return this.sendQueryRequest([], httpOpts, query, 'main'); // cursor mark or charts
      })
      .values();

    const series: any = {};
    const resultSeries: any[] = [];

    return Promise.all(targetPromises).then(responses => {
      responses.forEach(resp => {
        resp.forEach((r: any) => {
          r.data.forEach((s: any) => {
            if (s.type === 'table') {
              resultSeries.push(s);
            } else {
              series[s.target] = !series[s.target] ? s.datapoints : series[s.target].concat(s.datapoints);
            }
          });
        });
      });

      _.keys(series).forEach(key => {
        resultSeries.push({
          target: key,
          datapoints: series[key].sort((a: any, b: any) => {
            return a[1] - b[1];
          }),
        });
      });

      const result = {
        data: resultSeries,
      };

      return result;
    });
  }

  testDatasource() {
    //GET  "http://localhost:8082/topics"
    const options = {
      url: this.baseUrl + '/info',
      method: 'GET',
    };
    return this.backendSrv
      .datasourceRequest(options)
      .then((response: any) => {
        if (response.status === 200) {
          return {
            status: 'success',
            message: 'Data source is working',
            title: 'Success',
          };
        } else {
          return {
            status: 'error',
            message: 'Data source is NOT working',
            title: 'Error',
          };
        }
      })
      .catch((error: any) => {
        return {
          status: 'error',
          message: error.status + ': ' + error.statusText,
          title: 'Error',
        };
      });
  }

  sendQueryRequest(respArr: any[], options: any, query: BoltQuery, requestType: any, cursor?: any) {
    return this.backendSrv
      .datasourceRequest(options)
      .then((response: any) => {
        if (response.status === 200) {
          //const groupMap = this.jobIdMappings;

          const processedData = Utils.processResponse(response, this.templateSrv);

          respArr.push(processedData);

          return respArr;
        } else {
          return Promise.reject([
            {
              status: 'error',
              message: 'Error',
              title: 'Error',
            },
          ]);
        }
      })
      .catch((error: any) => {
        return Promise.reject([
          {
            status: 'error',
            message: error.status + ': ' + error.statusText,
            title: 'Error while accessing data',
          },
        ]);
      });
  }

  getQueryString(query: BoltQuery, options: DataQueryRequest<BoltQuery>) {
    let q: string;
    const queryStr = this.templateSrv.replace(query.query, options.scopedVars);
    const matches = queryStr.match(/__dashboard__:\s*(.*)/);
    let matches2 = queryStr.match(/__panel__:\s*(.*) AND .*/);
    if (!matches2) {
      matches2 = queryStr.match(/__panel__:\s*(.*)/);
    }

    if (matches && matches.length === 2) {
      // Dashboard case
      const dahsboardName: string = matches[1];
      if (dahsboardName === '*') {
        q = queryStr.replace('__dashboard__', 'jobId');
      } else if (dahsboardName.startsWith('{')) {
        // All option
        const jobIdList: any[] = [];
        const dashboards = dahsboardName
          .replace('{', '')
          .replace('}', '')
          .split(',');

        dashboards.forEach(dashboard => {
          const jobId: string[] = Object.keys(this.jobIdMappings.dashboards).filter(jobId => {
            return this.jobIdMappings.dashboards[jobId] === dashboard;
          });

          if (jobId) {
            jobId.forEach(job => jobIdList.push(job));
          }
        });

        const jobIdStr = '(' + jobIdList.join(' OR ') + ')';
        q = queryStr.replace('__dashboard__', 'jobId').replace(dahsboardName, jobIdStr);
      } else {
        // particular option
        const jobIdList: string[] = Object.keys(this.jobIdMappings.dashboards).filter((jobId: string) => {
          return this.jobIdMappings.dashboards[jobId] === dahsboardName;
        });

        const jobIdStr = '( ' + jobIdList.join(' OR ') + ' )';
        q = queryStr.replace('__dashboard__', 'jobId').replace(dahsboardName, jobIdStr);
      }
      q = Utils.queryBuilder(q);
    } else if (matches2 && matches2.length === 2) {
      // Panel case
      const panelName: string = matches2[1];

      if (panelName === '*') {
        q = queryStr.replace('__panel__', 'jobId');
      } else if (panelName.startsWith('{')) {
        // All option
        const jobIdList: any[] = [];
        const panels = panelName
          .replace('{', '')
          .replace('}', '')
          .split(',');

        panels.forEach(panel => {
          const jobId = Object.keys(this.jobIdMappings.panels).filter(jobId => {
            return this.jobIdMappings.panels[jobId] === panel;
          });

          if (jobId) {
            jobIdList.push(jobId);
          }
        });

        const jobIdStr = '(' + jobIdList.join(' OR ') + ')';
        q = queryStr.replace('__panel__', 'jobId').replace(panelName, jobIdStr);
      } else {
        // particular option
        const jobIdList: string[] = Object.keys(this.jobIdMappings.panels).filter((jobId: string) => {
          return this.jobIdMappings.panels[jobId] === panelName;
        });

        const jobIdStr = '( ' + jobIdList.join(' OR ') + ' )';
        q = queryStr.replace('__panel__', 'jobId').replace(panelName, jobIdStr);
      }
      q = Utils.queryBuilder(q);
    } else {
      q = Utils.queryBuilder(queryStr);
    }

    return q;
  }
}

export default BoltDatasource;
