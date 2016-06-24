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
    const isHist = this.state.buckets.length == 1;
    const heatOnly = (function(isHist, out) { return isHist ? null : out; }).bind({}, isHist);
    const histHeat = (function(isHist, hist, heat) { return isHist ? hist : heat; }).bind({}, isHist);
    const bucketsX = this.state.buckets[0];
    const bucketsY = histHeat([], this.state.buckets[1]);
    console.log("IS HIST", isHist);
    const data = isHist ?
      this.makeHistData(this.state.bucketizedCounts) :
      this.makeData(this.state.bucketizedCounts);

    const el = ReactDOM.findDOMNode(this);
    const margin = { top: 40, right: 0, bottom: 50, left: 90 },
          width = this.state.vizWidth - margin.left - margin.right,
          height = this.state.height - margin.top - margin.bottom,
          gridWidth = width / bucketsX.length,
          gridHeight = histHeat(height/2, height / bucketsY.length),
          legendElementWidth = Math.min(gridWidth*2, 80),
          buckets = 9,
          colors = ["#ffffd9","#edf8b1","#c7e9b4","#7fcdbb","#41b6c4","#1d91c0","#225ea8","#253494","#081d58"]; // alternatively colorbrewer.YlGnBu[9]
    d3.select("#heatmap-svg").remove();

    const svg = d3.select("#heatmap").append("svg")
        .attr("id","heatmap-svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    const histLabels = 
      [d3.max(data, function(d) { return d.value; }).toFixed(2),  
      d3.max(data, function(d) { return d.value; }).toFixed(2) / 2,
      0];

    const YLabels = svg.selectAll(".x-labels")
          .data(histHeat(histLabels, bucketsY))
          .enter().append("text")
            .text(function (d) { return d; })
            .attr("x", 0)
            .attr("y", function (d, i) { return i * gridHeight; })
            .style("text-anchor", "end")
            .attr("transform", "translate(-6," + 0 + ")")
            .attr("class", "dayLabel mono axis");
    const XLabels = svg.selectAll(".timeLabel")
          .data(bucketsX)
          .enter().append("text")
            .text(function(d) { return d; })
            .attr("x", function(d, i) { return i * gridWidth; })
            .attr("y", histHeat(height+20, 0))
            .style("text-anchor", "middle")
            .attr("transform", "translate(" + gridWidth/ 2 + ", -6)")
            .attr("class", "timeLabel mono axis");

    const colorScale = d3.scale.linear()
            .domain([d3.min(data, function (d) { return d.value; }), d3.max(data, function (d) { return d.value; })])
            .range(['#fde0dd', '#c51b8a']); 
    const cards = svg.selectAll(".hour")
            .data(data, function(d) {return d.varX+heatOnly(':'+d.varY);});

    cards.append("title");

    cards.enter().append("rect")
            .attr("x", function(d) { return (d.posX) * gridWidth; })
            .attr("y", function(d) { return histHeat(height*(1-(d.value / d3.max(data, function(dat) {return dat.value; }))), (d.posY) * gridHeight); })
            .attr("rx", 4)
            .attr("ry", 4)
            .attr("class", "hour bordered")
            .attr("width", gridWidth)
            .attr("height", function(d) { return histHeat(height * (d.value / d3.max(data, function(dat) {return dat.value; })), gridHeight); })
            .style("fill", function(d) { return histHeat('rgb(223, 120, 177)', colorScale(d.value)); });

    cards.select("title").text(function(d) { return d.value; });

    cards.exit().remove();

    
    if (!isHist) {
      var legend = svg.selectAll(".legend")
          .data([d3.min(data, function (d) { return d.value; }), d3.max(data, function (d) { return d.value; })])

      legend.enter().append("g")
          .attr("class", "legend");

      legend.append("rect")
        .attr("x", function(d, i) { return (width / 2) * i; })
        .attr("y", height + 20)
        .attr("width", (width / 2))
        .attr("height", 20)
        .style("fill", function(d, i) { return ['#fde0dd', '#c51b8a'][i]; });

      legend.append("text")
        .attr("class", "mono")
        .text(function(d, i) { return ['least frequent', 'most frequent'][i] + ": " + d; })
        .attr("x", function(d, i) { return (width / 2) * i; })
        .attr("y", height + 15);

      legend.exit().remove();
    }

  }

  makeHistData(bucketizedCounts) {
    const output = [];
    for (const key in bucketizedCounts) {
      const varX = key;
      const value = bucketizedCounts[key];
      output.push({
        'varX': varX, 
        'posX': this.state.buckets[0].indexOf(varX), 
        'value': value,
      });
    }
    return output;
  }

  makeData(bucketizedCounts) {
    const output = [];
    for (const key in bucketizedCounts) {
      const varX = key.split(",")[0];
      const varY = key.split(",")[1];
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
