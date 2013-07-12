angular
    .module('processMonitor.services', ['ngResource'])

    .factory('configService', function() {
        return {
            PROCESS_TREE_UPDATE_DELAY: 1000,
            CPU_UPDATE_DELAY: 300,
            UPDATE_DELTA: 1000,
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
                "size",
                "time_of_stat"
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
                    ofile.descriptors = _.map(open_file,
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

        var initBooleans = function(proc) {
            proc.is_master = (proc.pid == status_all.master_pid);
            proc.is_running = true;
            return proc;
        }

        // create process history node, populate it with values defined in PROCESS_HISTORY_KEYS
        var initProcessHistory = function(proc) {
            proc.history = [];
            var keys = configService.PROCESS_HISTORY_KEYS;
            var history = {};
            history.index = status_all.calls;
            _.each(keys, function(key) {
                history[key] = proc[key];
            });
            proc.history.push(history);
            return proc;
        }

        // create file history node(s), populate it with values defined in FILE_HISTORY_KEYS
        var initFileDataHistory = function(proc) {
            var keys = configService.FILE_HISTORY_KEYS;

            _.each(proc.files, function(file) {
                _.each(file.descriptors, function(descriptor){
                    descriptor.history = [];
                    var history = {};
                    history.index = status_all.calls;
                    history.pos_time = proc.time;

                    _.each(keys, function(key) {
                        history[key] = descriptor[key];
                    });

                    descriptor.history.push(history);
                });
            });
            return proc;
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
            initBooleans,
            initFileData,
            initProcessHistory,
            initFileDataHistory
        );

        /*
         * MAIN FUNCTIONS
         */

        // periodically polls processService, initializes current processes,
        // updates status_all, and nests processes for the treeview
        var poller = function() {
            console.log("poller() called.");

            processService.get()
                .then(function(processes) { // transform processService response to an array of initialized processes, set the timeout
                    var current_processes = _.map(processes, function(process) {
                        return initProcessPipeline(process);
                    });
                    $timeout(poller, configService.UPDATE_DELTA);
                    return current_processes;
                })
                .then(function(processes){ // get basic process info, merge with process details from processService
                    // create all the deferred basicService calls
                    var basic_deferreds = _.map(processes, function(process){
                        return basicService.get(process.pid);
                    });

                    // resolve them all with $q.all()
                    $q.all(basic_deferreds).then(function(results){
                        // merge status and basic nodes
                        status_current.processes = _.map(processes, function(process) {
                            return _.deepExtend(process, _.findWhere(results, { "pid": process.pid }));
                        })

                        // merge current process nodes into their corresponding status_all.processes nodes
                        _.each(status_current.processes, function(sc_process) {
                            var sa_process = _.findWhere(status_all.processes, { "pid": sc_process.pid });
                            if(_.isObject(sa_process)) {
                                _.mergeProcess(sa_process, sc_process); // exists, merge it
                            } else {
                                status_all.processes.push(cloneObj(sc_process)); // doesn't exist, add it
                            }
                        });

                        // find dead processes and toggle is_running to false
                        var sa_running = _.filter(status_all.processes, function(process) {
                            return process.is_running == true;
                        });
                        _.each(sa_running, function(process) {
                            if (!_.findWhere(status_current.processes, {"pid": process.pid})) {
                                process.is_running = false;
                            }
                        });

                        // nest status_all.processes to create status_processes
                        // status_processes.processes = _.nest(cloneObj(status_all.processes), 'parent_pid');
                    });
                });

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


