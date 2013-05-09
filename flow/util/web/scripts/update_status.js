var NUM_DATA_PTS = 1000;
var UPDATE_DELTA = 250; // ms

var ARRAY_FIELDS = [
    'cpu_percent',
    'cpu_user',
    'cpu_system',

    'memory_percent',
    'memory_rss',
    'memory_vms',
];

var SCALAR_FIELDS = [
];

// process_status is the main data-model object:
//   it is a dictionary of pid:info
//
//   pid1: {'cpu_percent':[0..num_data_pts],
//          'cpu_user':[0..num_data_pts],
//          ... <ARRAY_FIELDS> ...,
//          'file_info':{<filename1>:{<fh1>:{'size':[0..NUM_DATA_PTS],
//                                           'pos':[0..NUM_DATA_PTS],
//                                           'flags':man open(2),
//                                           'read_only':bool},
//                                    <fh2>: ...},
//                       <filename2>: ...},
//         },
//   pid2: ...},
var process_status = {};
var master_pid = 0;


var initialize_process = function (url) {
    $.getJSON(url, function (data) {
        if ('pid' in data) {
            var pid = data['pid'];

            if (url == '/basic') {
                master_pid = pid
            }

            if (!(pid in process_status)) {
                process_status[pid] = {'is_running':true};
            }

            for (var field in data) {
                process_status[pid][field] = data[field];
            }
        }
    });
}


var update_status = function() {
    $.getJSON('/status', _update_process_from_data)
        .fail(function (data, textStatus, jqXHR) {
                alert("Could not retrieve process status. " +
                      "The process has most likely completed. " +
                      "This monitor will continue to function but it will not recieve any more updates.");
        })
        .success(function () {setTimeout(update_status, UPDATE_DELTA)});
}

var _update_process_from_data = function(data) {
    for (var pid in data) {
        var pinfo = data[pid]
        if (!(pid in process_status)) {
            process_status[pid] = {'is_running':true};
            initialize_process( sprintf('/basic/%s', pid) )
        }

        _store_file_info(data, pid)
        for (var field in pinfo) {
            if ($.inArray(field, ARRAY_FIELDS) != -1) {
                if (!(field in process_status[pid])) {
                    process_status[pid][field] = {'values':[], 'key':field};
                }
                _store_array(pinfo[field], process_status[pid][field], pinfo['time'])
            } else if ($.inArray(field, SCALAR_FIELDS) != -1) {
                process_status[pid][field] = pinfo[field];
            }
        }
    }

    for (var pid in process_status) {
        if (!(pid in data)) {
            process_status[pid]['is_running'] = false;
        }
    }
}

var _store_array = function(src, dest, time) {
    values = dest['values']
    values.push({x:time, y:src});
    if (values.length > NUM_DATA_PTS) {
        values.splice(0,1);
    }
}

var _store_file_info = function(data, pid) {
    if (!('open_files' in process_status[pid])) {
        process_status[pid]['open_files'] = {}
    }
    stored_finfo = process_status[pid]['open_files']

    // note files that are no longer open
    finfo = data[pid]['open_files'] || {}
    for (var fname in stored_finfo) {
        for (var fd in stored_finfo[fname]) {
            if (!(fname in finfo)) {
                stored_finfo[fname][fd].closed = true
            } else if (!(fd in finfo[fname])) {
                stored_finfo[fname][fd].closed = true
            }
        }
    }

    // store info about currently open files
    for (var fname in finfo) {
        if (!(fname in stored_finfo)) {
            stored_finfo[fname] = {}
        }
        for (var fd in finfo[fname]) {
            if (!(fd in stored_finfo[fname])) {
                var stat_info = {
                    'type':finfo[fname][fd]['type'],
                    'read_only':finfo[fname][fd]['read_only'],
                    'flags':finfo[fname][fd]['flags'],
                    'closed':false,
                }
                stored_finfo[fname][fd] = stat_info
            }
            this_stored_finfo = stored_finfo[fname][fd]

            // size
            if (!('size' in this_stored_finfo)) {
                this_stored_finfo['size'] = {'values':[], 'key':'size'}
            }
            _store_array(finfo[fname][fd]['size'], this_stored_finfo['size'], finfo[fname][fd]['time_of_stat'])

            // pos
            if (!('pos' in this_stored_finfo)) {
                this_stored_finfo['pos'] = {'values':[], 'key':'pos'}
            }
            _store_array(finfo[fname][fd]['pos'], this_stored_finfo['pos'], data[pid]['time'])
        }
    }
}


initialize_process('/basic');
update_status();
