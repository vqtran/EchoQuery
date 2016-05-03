import React from 'react';
import ReactDOM from 'react-dom';
import d3 from 'd3';

import WindowStore from '../stores/WindowStore';
import SessionStore from '../stores/SessionStore';

class HeatMap extends React.Component {

  constructor(props) {
    super(props);
    this.onChange = this.onChange.bind(this);
    this.state = {...WindowStore.getState(), ...SessionStore.getState()}
  }

  render() {
    return (
      <div id="heatmap"></div>
    );
  }

  componentDidMount() {
    WindowStore.listen(this.onChange);
    SessionStore.listen(this.onChange);
    this.drawChart();
  }

  componentDidUpdate() {
    this.drawChart();
  }

  componentWillUnmount() {
    WindowStore.unlisten(this.onChange);
    SessionStore.unlisten(this.onChange);
  }

  onChange(state) {
    this.setState(state);
    setTimeout(this.drawChart(), 0);
  }

  drawChart() {
    const bucketsX = this.state.buckets[0];
    const bucketsY = this.state.buckets[1];
    const data = this.makeData(this.state.bucketizedCounts);

    const el = ReactDOM.findDOMNode(this);
    const margin = { top: 50, right: 0, bottom: 100, left: 30 },
          width = this.state.vizWidth - margin.left - margin.right,
          height = this.state.height - margin.top - margin.bottom,
          gridWidth = width / bucketsX.length,
          gridHeight = height / bucketsY.length,
          buckets = 9,
          colors = ["#ffffd9","#edf8b1","#c7e9b4","#7fcdbb","#41b6c4","#1d91c0","#225ea8","#253494","#081d58"]; // alternatively colorbrewer.YlGnBu[9]
    d3.select("#heatmap-svg").remove();

    const svg = d3.select("#heatmap").append("svg")
        .attr("id","heatmap-svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    const YLabels = svg.selectAll(".x-labels")
          .data(bucketsY)
          .enter().append("text")
            .text(function (d) { return d; })
            .attr("x", 0)
            .attr("y", function (d, i) { return i * gridHeight; })
            .style("text-anchor", "end")
            .attr("transform", "translate(-6," + gridHeight / 1.5 + ")")
            .attr("class", "dayLabel mono axis");
    const XLabels = svg.selectAll(".timeLabel")
          .data(bucketsX)
          .enter().append("text")
            .text(function(d) { return d; })
            .attr("x", function(d, i) { return i * gridWidth; })
            .attr("y", 0)
            .style("text-anchor", "middle")
            .attr("transform", "translate(" + gridWidth/ 2 + ", -6)")
            .attr("class", "timeLabel mono axis");

    const colorScale = d3.scale.linear()
            .domain([0, buckets - 1, d3.max(data, function (d) { return d.value; })])
            .range(colors); 
    const cards = svg.selectAll(".hour")
            .data(data, function(d) {return d.varX+':'+d.varY;});

    cards.append("title");

    cards.enter().append("rect")
            .attr("x", function(d) { return (d.posX) * gridWidth; })
            .attr("y", function(d) { return (d.posY) * gridHeight; })
            .attr("rx", 4)
            .attr("ry", 4)
            .attr("class", "hour bordered")
            .attr("width", gridWidth)
            .attr("height", gridHeight)
            .style("fill", function(d) { return colorScale(d.value); });

    cards.select("title").text(function(d) { return d.value; });

    cards.exit().remove();
  }

  makeData(bucketizedCounts) {
    const output = [];
    for (const key in bucketizedCounts) {
      const varX = key.split(",")[0];
      const varY = +key.split(",")[1];
      const value = bucketizedCounts[key];
      output.push({
        'varX': varX, 
        'posX': this.state.buckets[0].indexOf(varX), 
        'varY': varY, 
        'posY': this.state.buckets[1].indexOf(varY), 
        'value': value,
      });
    }
    return output;
  }

}

export default HeatMap;
