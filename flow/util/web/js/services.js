angular
    .module('processMonitor.services', ['ngResource'])

    .factory('configService', function() {
        // holds important app parameters
        console.log("configService instantiated.");

        return {
            process_tree_update_delay: 1000,
            cpu_update_delay: 300,
            update_delta: 5000,
            num_data_pts: 1000,

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
        console.log("processStartupService initialized.");
        var currentStatus = { response: {} };

        var allStatus = { calls: Number(), master_parent_pid: Number(), master_pid: Number(), processes: {} };

        var poller = function() {
            $http.get('/status').then(
                function(r) { // success
                    currentStatus.response = r.data;
                    updateAllStatus(r.data);
                    $timeout(poller, configService.update_delta);
                },
                function(r) { // fail
                    alert("Could not retrieve process status. " +
                          "The process has most likely completed. " +
                          "This monitor will continue to function but it will not recieve any more updates.");
                }

            );

        };

        poller();

        var updateAllStatus = function(status_data) {
            if (0 === allStatus.calls) {
                // set master_pids
                var basic = basicService
                    .getBasic()
                    .then(function(data) {
                        allStatus.master_pid = data.pid;
                        allStatus.master_parent_pid = data.parent_pid;
                });

            }

            allStatus.calls++;

            for (var pid in status_data) {
                basicService
                    .getBasic(pid)
                    .then(
                        function(basic_data) {
                            integrateNode(basic_data, status_data);
                        });



                var integrateNode = function(basic_data, status_data) {
                    var pid = basic_data.pid;
                    console.log('integrating node: ' + pid);

                    // if pid is not in allStatus, set its is_running to true
                    // then copy its fields over to allStatus
                    if (!(pid in allStatus)) {
                        allStatus.processes[pid] = { 'is_running':true };
                    }

                    // copy basic node data
                    for (var field in basic_data) {
                        allStatus.processes[pid][field] = basic_data[field];
                    }

                    // copy status node data
                    for (var field in status_data) {
                        allStatus.processes[pid][field] = status_data[field];
                    }

                    // storeFileInfo(status_data, pid);

                };
            }

        };

        var storeFileInfo = function(data, pid) {
            // check for existence of open_files for this process
            if(!_.has(allStatus.processes.pid, 'open_files')) {
                console.log("process " + pid + " has no open_files key.");
            }

            if(!_.has(allStatus.proccess.pid, 'memory_percent')) {
                console.log("process " + pid + " has no memory_percent key.");
            }

            // if (!('open_files' in allStatus[pid])) {
            //     allStatus[pid]['open_files'] = {};
            // }

            // note files that are no longer open
            // var file_info = data[pid]['open_files'] || {};
            // var stored_file_info = allStatus[pid]['open_files'];

            // for (var fname in stored_file_info) {
            //     console.log("updating file " + fname);
            //     for (var fd in stored_file_info[fname]) {
            //         if (!(fname in file_info)) {
            //             stored_file_info[fname][fd].closed = true;
            //         } else if (!(fd in file_info[fname])) {
            //             stored_file_info[fname][fd].closed = true;
            //         }
            //     }
            // }

        };

        var _store_array = function(src, dest, time) {
            var values = dest['values'];
            values.push({x:time, y:src});
            if (values.length > configService.num_data_pts) {
                values.splice(0,1);
            }
        };

        var _store_file_info = function(data, pid) {
            if (!('open_files' in allStatus[pid])) {
                allStatus[pid]['open_files'] = {};
            }

            var stored_finfo = allStatus[pid]['open_files'];

            // note files that are no longer open
            var finfo = data[pid]['open_files'] || {};
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
                    _store_array(finfo[fname][fd]['size'], this_stored_finfo['size'], finfo[fname][fd]['time_of_stat']);

                    // pos
                    if (!('pos' in this_stored_finfo)) {
                        this_stored_finfo['pos'] = {'values':[], 'key':'pos'};
                    }
                    _store_array(finfo[fname][fd]['pos'], this_stored_finfo['pos'], data[pid]['time']);
                }
            }
        };

        return {
            currentStatus: currentStatus,
            allStatus: allStatus
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
                        alert("Could not retrieve basic data node.");

                        deferred.reject();
                    });

                return deferred.promise;
            }
        };
    });
