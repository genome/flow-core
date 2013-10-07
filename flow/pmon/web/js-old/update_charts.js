
// just to help with formatting x axis
var time_to_string = function(t) {
    var dt = new Date(0);
    dt.setUTCSeconds(t)
    return dt.toLocaleTimeString()
}


var chart1;
nv.addGraph(function() {
  chart1 = nv.models.lineChart();

  chart1.xAxis
      .tickFormat(time_to_string);

  chart1.yAxis
      .axisLabel('Percent')

  return chart1;
});

var update_cpu_chart1 = function() {
    d1 = process_status[master_pid]['cpu_percent']

    d3.select('#chart1 svg')
      .datum([d1])
      .transition().duration(0)
      .call(chart1);
    setTimeout(update_cpu_chart1, UPDATE_DELTA);
}


var chart2;
nv.addGraph(function() {
  chart2 = nv.models.lineChart();

  chart2.xAxis
      .tickFormat(time_to_string);

  chart2.yAxis
      .axisLabel('Percent')

  return chart2;
});

var update_cpu_chart2 = function() {
    d1 = process_status[master_pid]['memory_percent']

    d3.select('#chart2 svg')
      .datum([d1])
      .transition().duration(0)
      .call(chart2);
    setTimeout(update_cpu_chart2, UPDATE_DELTA);
}


setTimeout(update_cpu_chart1, UPDATE_DELTA * 2);
setTimeout(update_cpu_chart2, UPDATE_DELTA * 2);
