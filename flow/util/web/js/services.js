angular
    .module('processMonitor.services', ['ngResource'])

    .factory('configService', function() {
        return {
            PROCESS_TREE_UPDATE_DELAY: 1000,
            CPU_UPDATE_DELAY: 300,
            UPDATE_DELTA: 5000,
            NUM_DATA_PTS: 1000,
            PROCESS_HISTORY_KEYS: [ // these process attribute histories will be stored for generating charts
                "memory_percent",
                "memory_vms",
                "memory_rss",
                "cpu_user",
                "cpu_system",
                "cpu_percent"
            ],
            FILE_HISTORY_KEYS:  [ // these file attribute histories will be stored for generating charts
                "pos",
                "size"
            ]
        };
    })

    .factory('statusService', function ($q, $http, $timeout, configService, processService, basicService) {

        /*
        * MODEL DATA STRUCTURES
         */

        // the current initialized status
        var status_current = { };

        // all initialized status updates with histories
        var status_all = {
            "calls": Number(),
            "master_parent_pid": Number(),
            "master_pid": Number(),
            "processes": []
        };

        // references to status_all, nested using angularTree's name: "", children: [] schema
        var status_processes = {
            "processes": [
                {
                    "name": "Object 1" ,
                    "children": [
                        {
                            "name": "Object 1.1",
                            "children": []
                        }

                    ]
                }
            ]
        };

        /*
         * PIPELINE FUNCTIONS
         */

        // initialize process file node
        var initFileData = function(proc) {
            // for each open_files, convert its fd to an array of objects
            proc.files = _.map(proc.open_files,
                function(open_file, key) {
                    var ofile = {};
                    ofile.name = key;
                    ofile.fd = _.map(open_file,
                        function(fd, key) {
                            var nfd= cloneObj(fd);
                            nfd['id'] = key;
                            return nfd;
                        })
                    return ofile;
                });

            delete proc.open_files;
            return proc;
        }

        // create process history node, populate it with values defined in process_history_keys
        var initProcessHistory = function(proc) {

        }

        // create file history node(s), populate it with values defined in file_history_keys
        var initFileDataHistory = function(proc) {

        }

        // find the difference of status_current and status_all processes, set all their is_running bools to false
        var setIsRunning = function(stat_current) {

        }

        // find the difference of status_current and status_all files, set all their is_open bools to false
        var setIsOpen = function(stat_current) {

        }

        /*
         * UTILITIES
         */
        // fastest clone, but clobbers functions (use _.deepExtend(source_obj, {}) to preserve functions)
        var cloneObj = function(obj) {
            return JSON.parse(JSON.stringify(obj));
        }

        /*
         * PIPELINES
         */

        // pipeline to initialize a service_data.data object to a status_current.processes object
        var initProcessPipeline = _.pipeline(
            initFileData,
            initProcessHistory,
            initFileDataHistory
        );

        /*
         * MAIN FUNCTIONS
         */

        // periodically polls processService, calls various process data structure pipelines
        var poller = function() {
            console.log("poller() called.");
            var deferred_calls = [];

            processService.get()
                .then(function(processes) {
                    console.log("executing processService.get()");
                    status_current['processes'] = _.map(processes, function(process) { return process; } );
                    $timeout(poller, configService.UPDATE_DELTA);
                    return status_current.processes;
                })
                .then(function(processes){
                    console.log("Second processService then() hit.");
                    console.log(processes);
                    // create all the deferred basicService calls
                    var basic_deferreds = _.map(processes, function(process){
                        return basicService.get(process.pid);
                    })
                    // resolve them all with $q.when()
                    $q.all(basic_deferreds).then(function(results){
                        console.log("$q.when().then() hit.");
                        console.log(results);
                        status_all.processes = results;
                    });
                    // in the $q.when().then clause, merge the basic w/ the status
                });

//            var basic_deferred = $q.all(deferred_calls);
//
//            $q.when(basic_deferred).then(function(results){
//                // merge process status and basic nodes here.
//
//            });
            status_all.calls++;
        };

        return {
            "poller": poller,
            "status_current": status_current,
            "status_all": status_all,
            "status_processes": status_processes
        };
    })

    // returns an array of processes with merged /basic and /status data
    .factory('processService', function($http, $q, basicService) {
        return {
            get: function() {
                var deferred = $q.defer();
                var url = "/status";
                $http.get(url)
                    .success(function(data) { // Do something successful.
                        deferred.resolve(data);
                    })
                    .error(function(data) { // Handle the error
                        console.error("Could not retrieve status data.");
                        deferred.reject();
                    });
                return deferred.promise;
            }
        };

    })

    .factory('basicService', function($http, $q) {
        // returns basic info about a process
        return {
            get: function(pid) {
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


