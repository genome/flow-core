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

            master_pid: Number(),

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

    .factory('processStatusService', function($http, $timeout, configService, getBasicService) {
        // polls /status and updates process data node
        console.log("processStartupService initialized.");
        var currentStatus = { response: {} };
        var allStatus = { processes: {}, calls: 0 };

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

        var updateAllStatus = function(data) {
            allStatus.calls++;

            for (var pid in data) {
                getBasicService
                    .getBasic(pid)
                    .then(
                        function(data) {
                            //this will execute when the
                            //AJAX call completes.
                            //$scope.foo2 = data;
                            console.log("basic data ---------");
                            console.log(data);
                        });

                var pinfo = data[pid];


                if (!(pid in allStatus)) {
                    allStatus[pid] = {'is_running':true};
                    //initialize_process( sprintf('/basic/%s', pid) );
                }

                _store_file_info(data, pid);

                for (var field in pinfo) {
                    if ($.inArray(field, configService.array_fields) != -1) {
                        if (!(field in allStatus[pid])) {
                            allStatus[pid][field] = {'values':[], 'key':field};
                        }
                        _store_array(pinfo[field], allStatus[pid][field], pinfo['time']);
                    } else if ($.inArray(field, configService.scalar_fields) != -1) {
                        allStatus[pid][field] = pinfo[field];
                    }
                }
            }

            for (var pid in allStatus) {
                if (!(pid in data)) {
                    allStatus[pid]['is_running'] = false;
                }
            }
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

    .factory('getBasicService', function($http, $q) {
        // returns basic info about a process
        var basicData = {};
        return {
            getBasic: function(pid) {
                var url = pid ? '/basic/' + pid : '/basic';
                // console.log("url: " + url);

                var deferred = $q.defer();

                $http.get(url)
                    .success(function(data, status, headers, config) { // Do something successful.
                        // console.log("$http.get success! data:");
                        // console.log(data);

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
