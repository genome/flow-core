angular
    .module('processMonitor.services', ['ngResource'])

    .factory('configService', function() {
        // holds important app parameters
        // console.log("configService instantiated.");

        return {
            PROCESS_TREE_UPDATE_DELAY: 1000,
            CPU_UPDATE_DELAY: 300,
            UPDATE_DELTA: 5000,
            NUM_DATA_PTS: 1000,

            MASTER_PID: Number(),

            array_fields: [
                'cpu_percent',
                'cpu_user',
                'cpu_system',

                'memory_percent',
                'memory_rss',
                'memory_vms'
            ],
            scalar_fields: []
        };

    })

    // polls /status and updates process data node
    .factory('statusService', function($http, $timeout, configService, basicService) {

        var status_current = {
            "processes": []
        };

        var status_all = {
            "calls": Number(),
            "master_parent_pid": Number(),
            "master_pid": Number(),
            "processes": []
        }; // all initialized status updates

        var status_all_nested = {
            "calls": Number(),
            "master_parent_pid": Number(),
            "master_pid": Number(),
            "processes": []
        }; // all initialized status updates, nested by parent id

        // polls /status every UPDATE_DELTA
        var _poller = function() {
            var service_data = { };
            $http.get('/status').then(
                function(r) { // success
                    service_data.config = r.config;
                    service_data.data = r.data;
                    service_data.headers = r.headers;
                    service_data.status = r.status;

                    console.log("service_data");
                    console.log(service_data);

                    _update_status(service_data, status_current, status_all, status_all_nested);

                    $timeout(_poller, configService.UPDATE_DELTA);
                },
                function(r) { // fail
                    alert("Could not retrieve process status. " +
                        "The process has most likely completed. " +
                        "This monitor will continue to function but it will not recieve any more updates.");
                }

            );

        };

        _poller();

        var _update_status = function(service_data, status_current, status_all, status_all_nested) {
            // if this is the first time called, initialize status_all
            if (status_all.calls === 0) {
                var basic = basicService;
                basic.get()
                    .then(function(data) {
                        status_all.master_pid = data.pid;
                        status_all.master_parent_pid = data.parent_pid;
                    });
            }

            status_current.processes = [];
            status_current.processes = _.map(service_data.data,
                function(process) {
                    console.log("pid " + process.pid);

                    process.is_running = true;
                    process.is_master = (process.pid === status_all.master_pid);

                    // get parent_pid
                    var basic = basicService;
                    basic.get(process.pid)
                        .then(function(data) {
                            process.parent_pid = data.parent_pid;
                        });

                    // store file info

                    // store visualization array

                    return process;
                });

            // set all processes to false
            _.each(status_all.processes, function(process) {
                process.is_running = false;
            });

            _.deepExtend(status_all, status_current);
            // get all is_running processes from status_all
            // toggle process.is_running to false if not found in status_current.processes

            // merge status_current with status_all
            status_all_nested.processes = _.nest(status_all.processes, 'parent_pid');

            // copy status_all to status_all_nested then nest the processess


            status_all.calls++;
        };

        var _init_process = function(process) {
            console.log("_init_process called.");
            process['is_running'] = true;
            var pid = process.pid;
            _(process).each(function() {
                console.log("pid: " + pid);
                console.log(process);
            });

            // _init_open_files(process);
            // _init_chart_data(process);
        };

        var _init_open_files = function(process) {
            if (!('open_files' in process)) { process['open_files'] = { }; }

            var stored_finfo = process['open_files'];

            console.log("process " + process.pid + " open_files:");
            console.log(stored_finfo);
            // note files that are no longer open
            var finfo = process['open_files'] || {};
            for (var fname in stored_finfo) {
                for (var fd in stored_finfo[fname]) {
                    if (!(fname in finfo)) {
                        stored_finfo[fname][fd].closed = true;
                    } else if (!(fd in finfo[fname])) {
                        stored_finfo[fname][fd].closed = true;
                    }
                }
            }

            // store info about currently open fil]es
            for (var fname in finfo) {
                if (!(fname in stored_finfo)) {
                    stored_finfo[fname] = {};
                }
                for (var fd in finfo[fname]) {
                    if (!(fd in stored_finfo[fname])) {
                        var stat_info = {
                            'type':finfo[fname][fd]['type'],
                            'read_only':finfo[fname][fd]['read_only'],
                            'flags':finfo[fname][fd]['flags'],
                            'closed':false
                        };
                        stored_finfo[fname][fd] = stat_info;
                    }
                    var this_stored_finfo = stored_finfo[fname][fd];

                    // size
                    if (!('size' in this_stored_finfo)) {
                        this_stored_finfo['size'] = {'values':[], 'key':'size'};
                    }
                    // _store_array(finfo[fname][fd]['size'], this_stored_finfo['size'], finfo[fname][fd]['time_of_stat']);

                    // pos
                    if (!('pos' in this_stored_finfo)) {
                        this_stored_finfo['pos'] = {'values':[], 'key':'pos'};
                    }
                    // _store_array(finfo[fname][fd]['pos'], this_stored_finfo['pos'], data[pid]['time']);
                }
            }
        };

        var _init_chart_data = function(src, desc, time) {
            // console.log("_init_process_chart_data called.");
        };

        var _nest_all_status = function() {
            // console.log("_nest_all_status called.");
            all_status_nested = _.nest(all_status, ["parent_pid","id"]);
        };

        //
        // UTILITES
        //

        var _store_array = function(src, dest, time) {
            var values = dest['values'];
            values.push( { x:time, y:src } );
            if ( values.length > configService.NUM_DATA_PTS ) { values.splice(0,1); }
        };

        return {
            "status_current": status_current,
            "status_all": status_all,
            "status_all_nested": status_all_nested
        };


    })

    .factory('basicService', function($http, $q, configService) {
        // returns basic info about a process
        return {
            get: function(pid) {
                console.log("basicService.get() called.");
                var deferred = $q.defer();
                var url = pid ? '/basic/' + pid : '/basic';
                $http.get(url)
                    .success(function(data) { // Do something successful.
                        deferred.resolve(data);
                    })
                    .error(function(data) { // Handle the error
                        console.error("Could not retrieve basic data node.");
                        deferred.reject();
                    });

                return deferred.promise;
            }
        };
    });


