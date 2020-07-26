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

export class Utils {
  static processResponse(response: any, templateSrv: any) {
    const data: any[] = response.data;
    const header = data[0].header;

    const headerFields = header.schema.split(',').map((f: any) => f.match(/`(.*?)`/)[1]);

    const fieldsMap: any = {};
    const DataMap: any = {};
    let timeColumnIndex: number;
    headerFields.forEach((f: string, index: number) => {
      if (f === 'JOBID' || f === 'WINDOWEND') {
        return;
      } else if (f === 'WINDOWSTART') {
        timeColumnIndex = index;
        return;
      }
      fieldsMap[f] = index;
      DataMap[f] = [];
    });

    data.slice(1).forEach(d => {
      if (d.finalMessage) {
        return;
      }
      const rowData = d.row.columns;
      Object.keys(fieldsMap).forEach(field => {
        const index: number = fieldsMap[field];
        const value = rowData[index];

        DataMap[field].push([value, rowData[timeColumnIndex]]);
      });
    });

    let seriesList: any[] = [];
    if (Object.keys(DataMap).length > 0) {
      Object.keys(DataMap).forEach(metric => {
        seriesList.push({
          target: metric,
          datapoints: DataMap[metric],
        });
      });
    }

    if (!seriesList) {
      seriesList = [];
    }
    return {
      data: seriesList,
    };
  }

  static getGrouppedResults(seriesList: [], groupMap: any) {
    const groupSeriesList: any = {};
    const seriesListOutput: any[] = [];

    seriesList.forEach((series: any) => {
      const jobId = series.jobId;
      const datapoints: [] = series.datapoints;
      let dashboardName: string = groupMap.dashboards[jobId];

      if (!dashboardName) {
        dashboardName = jobId;
      }
      if (!groupSeriesList[dashboardName]) {
        groupSeriesList[dashboardName] = [];
      }

      datapoints.forEach((data, index) => {
        if (!groupSeriesList[dashboardName][index] || groupSeriesList[dashboardName][index][0] < data[0]) {
          groupSeriesList[dashboardName][index] = data;
        }
      });
    });

    Object.keys(groupSeriesList).forEach((dashboard: string) => {
      seriesListOutput.push({
        target: dashboard,
        datapoints: groupSeriesList[dashboard],
      });
    });

    return seriesListOutput;
  }

  static mapToTextValue(result: any) {
    if (result.data && result.data.collections) {
      return result.data.collections.map((collection: string) => {
        return {
          text: collection,
          value: collection,
        };
      });
    }
    if (result.data && result.data.facet_counts) {
      const ar: any[] = [];
      for (const key in result.data.facet_counts.facet_fields) {
        if (!result.data.facet_counts.facet_fields.hasOwnProperty(key)) {
          continue;
        }

        const array = result.data.facet_counts.facet_fields[key];
        for (let i = 0; i < array.length; i += 2) {
          // take every second element
          if (
            array[i + 1] > 0 &&
            !ar.find(ele => {
              return ele.text === array[i];
            })
          ) {
            let text = array[i];
            const detectorPatternMatches = text.match(/\( Function: .* Field: (.*) \)/);
            if (detectorPatternMatches) {
              text = detectorPatternMatches[1];
            }

            if (text) {
              text = text
                .replace(/\"/g, '\\"')
                .replace(/{/g, '\\{')
                .replace(/}/g, '\\}');
            }
            ar.push({
              text: '"' + text + '"',
              expandable: false,
            });
          }
        }
      }
      return ar;
    }
    if (result.data) {
      return result.data
        .split('\n')[0]
        .split(',')
        .map((field: string) => {
          return {
            text: field,
            value: field,
          };
        });
    }
  }

  static getFirstAndLastNResults(data: any, pageSize: any) {
    let arr: any[] = [];
    if (data && data.data && data.data.response) {
      for (let i = 0; i < Math.round(data.data.response.numFound / Number(pageSize.query)); i++) {
        arr.push(i);
      }
    }
    arr = arr.map(ele => {
      return {
        text: ele + 1,
        value: ele,
      };
    });
    const firstNResults = arr.slice(0, 10);
    const lastNResults = arr.splice(arr.length - 11, arr.length - 1);

    if (firstNResults.length === 0 && lastNResults.length === 0) {
      return [
        {
          text: 0,
          value: 0,
        },
      ];
    }

    return firstNResults.concat(lastNResults);
  }

  static sortList(seriesList: any[], top?: number) {
    seriesList.sort((a: any, b: any) => {
      let totalA = 0;
      let totalB = 0;
      if (a.datapoints && b.datapoints) {
        a.datapoints.map((d: any) => {
          totalA += d[0];
        });
        b.datapoints.map((d: any) => {
          totalB += d[0];
        });
      } else {
        return 0;
      }

      return totalB - totalA;
      // return b.score - a.score;
    });

    if (top) {
      seriesList = seriesList.slice(0, top);
    }

    return seriesList;
  }

  static getSortedSeries(seriesToSort: any[], baselineSeries: any[], indvAnOutField: string): any[] {
    const resultSeries: any[] = [];
    const seriesSuffixes = indvAnOutField === 'all' ? [' actual', ' expected', ' score', ' anomaly'] : [' ' + indvAnOutField];
    baselineSeries.forEach(baselineSer => {
      const seriesName = baselineSer.target;
      seriesSuffixes.forEach(suffix => {
        resultSeries.push(
          seriesToSort.find(s => {
            return s.target === seriesName + suffix;
          })
        );
      });
    });

    return resultSeries;
  }

  static getStdDev(series: number[]) {
    const min = Math.min(...series);
    const max = Math.max(...series);

    series = series.map(b => {
      return (b - min) / (max - min);
    });

    return series;
  }

  static findCorrelation(x: any, y: any) {
    let sumX = 0,
      sumY = 0,
      sumXY = 0,
      sumX2 = 0,
      sumY2 = 0;
    const minLength = (x.length = y.length = Math.min(x.length, y.length)),
      reduce = (xi: any, idx: any) => {
        const yi = y[idx];
        sumX += xi;
        sumY += yi;
        sumXY += xi * yi;
        sumX2 += xi * xi;
        sumY2 += yi * yi;
      };
    x.forEach(reduce);
    return (minLength * sumXY - sumX * sumY) / Math.sqrt((minLength * sumX2 - sumX * sumX) * (minLength * sumY2 - sumY * sumY));
  }

  static queryBuilder(query: string) {
    return (
      query
        .replace(/{/g, '(') // (?<!(?:\\)){ Replace { not followed by \ with (. Reverting this part as negative lookbehind
        //pattern doesn't work in Safari  and Solr treats { and ( same.
        .replace(/}/g, ')') // Replace } not followed by \ with )
        .replace(/\",\"/g, '" OR "')
    );
  }
}
