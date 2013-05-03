function objToString (obj) {
    var str = '...';
    for (var p in obj) {
            str += p + '::' + obj[p] + '\n';
    }
    return str;
}


var num_data_pts = 1000;
var ARRAY_FIELDS = [
    'cpu_percent',
    'cpu_user',
    'cpu_system',

    'memory_percent',
    'memory_rss',
    'memory_vms',
];

var process_status = {};
var master_pid = 0;
var initialize_process = function (url) {
    $.getJSON(url, function (data) {
        var pid = data['pid'];

        if (url == '/basic') {
            master_pid = pid
        }

        if (!(pid in process_status)) {
            process_status[pid] = {};
        }

        for (var field in data) {
            process_status[pid][field] = data[field];
        }
    });
}

initialize_process('/basic');

var update_process_from_data = function(data) {
    for (var pid in data) {
        pinfo = data[pid]
        if (!(pid in process_status)) {
            process_status[pid] = {};
            initialize_process( sprintf('/basic/%s', pid) )
        }

        for (var field in pinfo) {
            if ($.inArray(field, ARRAY_FIELDS) != -1) {
                if (field in process_status[pid]) {
                    process_status[pid][field]['values'].push({x:pinfo['time'], y:pinfo[field], size:0});
                    if (process_status[pid][field]['values'].length > num_data_pts) {
                        process_status[pid][field]['values'].splice(0,1);
                    }
                } else {
                    process_status[pid][field] = {values:[pinfo[field]], key:field};
                }
            } else {
            }
        }
    }
}

var update_status = function() {
    $.getJSON('/status', update_process_from_data);
    setTimeout(update_status, 100);

//   setTimeout(update_status, 1000);
}
update_status();

// Wrapping in nv.addGraph allows for '0 timeout render', stores rendered charts in nv.graphs, and may do more in the future... it's NOT required
var chart;

var time_to_string = function(t) {
    var dt = new Date(0);
    dt.setUTCSeconds(t)
    return dt.toLocaleTimeString()
}


nv.addGraph(function() {
  chart = nv.models.lineChart();

  chart.xAxis // chart sub-models (ie. xAxis, yAxis, etc) when accessed directly, return themselves, not the parent chart, so need to chain separately
      .tickFormat(time_to_string);

  chart.yAxis
      .axisLabel('Percentage')

  chart.forceY([0, 100]);

  return chart;
});

var update_cpu_chart = function() {
    cpu_percent = process_status[master_pid]['cpu_percent']
    cpu_percent.area = true

    memory_percent = process_status[master_pid]['memory_percent']
    memory_percent.area = true

    d3.select('#chart1 svg')
      .datum([cpu_percent, memory_percent])
      .transition().duration(0)
      .call(chart);
    setTimeout(update_cpu_chart, 100);
}
setTimeout(update_cpu_chart, 200);

