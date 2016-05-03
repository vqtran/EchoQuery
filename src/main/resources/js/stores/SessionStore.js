import alt from '../alt';

import DisplayActions from '../actions/DisplayActions';
import SessionActions from '../actions/SessionActions';

class SessionStore {

  constructor() {
    this.bindListeners({
      updateDisplayData: DisplayActions.setDisplayData,
      updateMode: DisplayActions.setMode,
      chooseColumns: DisplayActions.setColumns,
    });

    this.state = {
      userId: window.location.pathname.split("/")[2],
      displayData: {},
      responseHistory: ["What do you want?"],
      mode: "heatmap",
      chosenColumns: ["patients.age", "patients.gender"],
      buckets: [["none"], ["none"]],
      bucketizedCounts: [],
      bucketCount: 10,
    };

    this.exportPublicMethods({
      getSessionId: this.getSessionId,      
    });
  }

  calculateBuckets() {
    const newBuckets = this.state.buckets;
    for (const col in this.state.chosenColumns) {
      newBuckets[col] = this.calculateBucket(this.state.chosenColumns[col]);
    }
    this.setState({
      buckets: newBuckets,
      bucketizedCounts: this.generateBucketizedCounts(newBuckets),
    });
  }

  generateBucketizedCounts(buckets) {
    const output = [];
    for (let idx = 0; idx < this.state.displayData[this.state.chosenColumns[0]].length; idx++) {
      const key = [];
      for (const col in this.state.chosenColumns) {
        key.push(this.getBucket(this.state.displayData[this.state.chosenColumns[col]][idx], buckets[col]));
      }
      if (key in output) {
        output[key] += 1;
      } else {
        output[key] = 1;
      }
    }
    return output;
  }

  getBucket(val, buckets) {
    switch (typeof val) {
      case 'string':
        return val;
      case 'number':
        for (let bucket of buckets.slice().reverse()) {
          if (val >= bucket) {
            return bucket;
          }
        }
    }
  }

  calculateBucket(col) {
    switch (typeof this.state.displayData[col][0]) {
      case 'string':
        return [...new Set(this.state.displayData[col])];
      case 'number':
        const localmin = Math.min(...this.state.displayData[col]);
        const localmax = Math.max(...this.state.displayData[col]);
        const localdiff = localmax - localmin;
        return this.range(localmin, localmax, localdiff / this.state.bucketCount);
      default:
        return ["none"];
    }
  }

  range(start, stop, step) {
    if (stop == null) {
      stop = start || 0;
      start = 0;
    }
    step = step || 1;

    var length = Math.max(Math.ceil((stop - start) / step), 0);
    var range = Array(length);

    for (var idx = 0; idx < length; idx++, start += step) {
      range[idx] = start;
    }

    return range;
  };

  updateDisplayData(data) {
    this.setState({
      displayData: JSON.parse(data["sessions.result"]),
      responseHistory: this.possiblyAppend(data["sessions.display"][0], this.state.responseHistory),
    });
    this.calculateBuckets();
  }

  updateMode(data) {
    this.setState({
      mode: data,
    });
  }
  
  chooseColumns(data) {
    this.setState({
      chosenColumns: data,
    });
    this.calculateBuckets()
  }

  possiblyAppend(response, history) {
    if (response != history[0]) {
      return [response].concat(history);
    } else {
      return history
    }
  }

  getSessionId() {
    return this.state.userId;
  }
}

export default alt.createStore(SessionStore);
