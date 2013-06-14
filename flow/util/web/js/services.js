angular
    .module('processMonitor.services', ['ngResource'])

    .factory('configService', function() {
        // holds important app parameters
        console.log("configService instantiated.");

        return {
            process_tree_update_delay: 1000,
            cpu_update_delay: 300,
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

    .factory('statusService', function($http, $timeout, configService, basicService) {
        // polls /status and updates process data node
        console.log("statusService initialized. *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-");

        var UPDATE_DELTA = configService.UPDATE_DELTA;

        var current_status = { };
        var all_status = { calls: Number(), master_parent_pid: Number(), master_pid: Number(), processes: {} };
        var all_status_nested = { };

        var _poller = function() {
            $http.get('/status').then(
                function(r) { // success
                    // current_status.response = r.data;
                    _update_all_status(r.data);
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

        var _update_all_status = function(status_data) {
            // status_data: current status straight from /status
            // all_status: contains all status updates (initialized)
            // all_status_nested: status updates nested according to parent/child relationships

            // console.log("_update_all_status called.");
            // console.log("status_data:");
            // console.log(status_data);

            // if all_status hasn't been called, initialize it
            if (0 === all_status.calls) {
                // set master_pids
                var basic = basicService
                        .getBasic()
                        .then(function(data) {
                            all_status.master_pid = data.pid;
                            all_status.master_parent_pid = data.parent_pid;
                            all_status.processes[data.pid] = {}; // set master_pid as root process
                        });

            }

            // console.log("all_status:");
            // console.log(all_status);

            // console.log("current_status (before addint status_data):");
            // console.log(current_status);

            _.each(status_data, function(process_data) {

                console.log("process_data:");
                console.log(process_data);

                // get process' basic info, copy to current_status and initialize
                var basic = basicService
                        .getBasic()
                        .then(function(data) {
                            var cPid = data.parent_pid;
                            current_status[cPid] = {};
                            current_status[cPid] = _.deepClone(process_data);

                            // console.log("current_status[cPid]:-:-:-:-:-:-:-:-:");
                            // console.log(current_status[cPid]);

                            _init_process(current_status[cPid]);
                        });


            });

            all_status.calls++;
        };

        var _init_process = function(process) {
            process['is_running'] = true;

            _init_open_files(process);
            _init_chart_data(process);
            _nest_all_status();
        };

        var _init_open_files = function(process) {
            console.log("_init_process_open_files called. -- -- -- -- -- -- -- --");
            console.log(process);

            if (!('open_files' in process)) { process['open_files'] = { }; }

            var stored_finfo = process['open_files'];

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

            // store info about currently open files
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
                            'closed':false,
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
            console.log("_init_process_chart_data called.");
        };

        var _nest_all_status = function() {
            console.log("_nest_all_status called.");
            all_status_nested = _.nest(all_status, ["parent_pid","id"]);
        };

        //
        // UTILITES
        //

        var _quick_clone = function(obj) { return JSON.parse(JSON.stringify(obj)); };

        var _store_array = function(src, dest, time) {
            var values = dest['values'];
            values.push( { x:time, y:src } );
            if ( values.length > configService.NUM_DATA_PTS ) { values.splice(0,1); }
        };

        return {
            current_status: current_status,
            all_status: all_status,
            all_status_nested: all_status_nested
        };


    })

    .factory('basicService', function($http, $q, configService) {
        // returns basic info about a process
        var basicData = {};
        return {
            getBasic: function(pid) {
                var deferred = $q.defer();
                var url = pid ? '/basic/' + pid : '/basic';
                $http.get(url)
                    .success(function(data, status, headers, config) { // Do something successful.
                        deferred.resolve(data);
                    })
                    .error(function(data, status, headers, config) { // Handle the error
                        console.error("Could not retrieve basic data node.");

                        deferred.reject();
                    });

                return deferred.promise;
            }
        };
    });
