angular.module('processMonitor.services', ['ngResource'])
    .factory('configService', function() {
        return {
            APP_NAME: "PTero Node Dashboard",
            VERSION: "0.1",
            CPU_UPDATE_DELAY: 300,
            UPDATE_DELTA: 1000,
            SAVE_HISTORY: 100,
            KEEP_DEAD_PROCESSES_FOR: 10,
            HOSTNAME: "",
            PROCESS_HISTORY_KEYS: [
                // these process attribute histories will be stored for generating charts
                "time",
                "memory_percent",
                "memory_vms",
                "memory_rss",
                "cpu_user",
                "cpu_system",
                "cpu_percent"
            ],
            FILE_HISTORY_KEYS:  [
                // these file attribute histories will be stored for generating charts
                "pos",
                "size",
                "time_of_stat"
            ],
            STATUS_PROCESSES_KEYS: [
                // these process attributes will be stored in the nested process
                // data structure that drives the process menu
                "cpu_percent",
                "memory_rss",
                "is_running",
                "is_master"
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

        // nested data structure of simplified processes, used to drive the process tree menu
        var status_processes = [];

        /*
         * PIPELINE FUNCTIONS
         */

        // initialize process file node
        var initFileData = function(proc) {
            // for each open_files, convert its fd to an array of objects
            proc.files = _.map(proc.open_files,
                function(open_file, key) {
                    var ofile = {};
                    ofile.is_open = true;
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
        };

        var initProcess = function(proc) {
            proc.is_master = (proc.pid === status_all.master_pid);
            proc.is_running = true;
            return proc;
        };

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
        };

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
        };

        /*
         * UTILITIES
         */
        // fastest clone, but clobbers functions (use _.deepExtend(source_obj, {}) to preserve functions)
        var cloneObj = function(obj) {
            return JSON.parse(JSON.stringify(obj));
        };

        var trimArray = function(array, trim_length) {
            if (array.length > trim_length) {
                for(var i = array.length - trim_length; i > 0; i--) {
                    var shift = array.shift();
                }
            }
        };

        /*
         * PIPELINES
         */

        // pipeline to initialize a service_data.data object to a status_current.processes object
        var initProcessPipeline = _.pipeline(
            initProcess,
            initFileData,
            initProcessHistory,
            initFileDataHistory
        );

        /*
         * MAIN FUNCTIONS
         */

        // periodically polls processService, initializes current processes,
        // updates status_all, and nests processes for the treeview process menu
        var poller = function() {
            // console.log("poller() called.");

            processService.get()
                .then(function(processes) { // transform processService response to an array of initialized processes
                    var current_processes = _.map(processes, function(process) {
                        return initProcessPipeline(process);
                    });

                    return current_processes;
                })
                .then(function(processes){
                    // create all the deferred basicService calls
                    var basic_deferreds = _.map(processes, function(process){
                        var currentProcess = _.findWhere(status_all.processes, { "pid": process.pid });
                        if (_.isObject(currentProcess) && _.has(currentProcess, 'parent_pid')) {
                            // return nothing - process exists and has already been merged w/ basic info
                            // (these undefined items are removed from basic_deferreds with _.compact later)
                            return undefined;
                        } else {
                            return basicService.get(process.pid);
                        }
                    });

                    // call with $q.all() the deferred objects we set up previously,
                    // then manage process data structures
                    $q.all(_.compact(basic_deferreds)).then(function(results){
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
                            if (!_.isObject(_.findWhere(status_current.processes, {"pid": process.pid}))) {
                                process.is_running = false;
                            }
                        });

                        // find closed files
                        var sa_files_open;
                        var sc_files_open;

                        _.each(status_all.processes, function(process) {
                            sa_files_open = _.map(process.files, function(file) {
                                if(file.is_open == true) { return file }
                            });
                        });

                        _.each(status_current.processes, function(process) {
                            sc_files_open = _.map(process.files, function(file) {
                                if(file.is_open == true) { return file }
                            });
                        });

                        _.each(sc_files_open, function(file) {
                            if (!_.findWhere(sa_files_open, {"name": file.name})) {
                                file.is_open= false;
                            }
                        });

                        // trim process and file histories
                        _.each(status_all.processes, function(process) {
                            var trim_len = configService.SAVE_HISTORY;
                            trimArray(process.history, trim_len);
                            _.each(process.files, function(file) {
                                _.each(file.descriptors, function(descriptor) {
                                    trimArray(descriptor.history, trim_len);
                                });
                            })
                        });

                        // cull dead processes
                        var to_be_culled = [];

                        var dead_processes = _.filter(status_all.processes,
                            function(process) {
                                return process.is_running == false
                            });

                        _.each(dead_processes,
                            function(process) {
                                if(_.has(process, 'age')) {
                                    process.age++;
                                } else {
                                    process.age = 1;
                                }

                                if (process.age > configService.KEEP_DEAD_PROCESSES_FOR) {
                                    to_be_culled.push(process);
                                    // FIND AND CULL THE CHILDREN
                                    var child_processes = _.filter(status.processes,
                                        function(sa_process) { return process.pid == sa_process.parent_pid});

                                    _.each(child_processes,
                                        function(cprocess) {
                                            // add to culled_processes if not already present
                                            if(!_.isObject(_.findWhere(to_be_culled, {"pid": cprocess.pid }))) {
                                                to_be_culled.push(cprocess);
                                            }
                                    });
                                }
                            });

                        status_all.processes = _.reject(status_all.processes,
                            function(process) {
                                return _.isObject(_.findWhere(to_be_culled, { "pid": process.pid }));
                            });

                        var createRefObj  = function(process) {
                            var ref_obj = {};

                            ref_obj['pid'] = process.pid;
                            ref_obj['children'] = findChildren(process);

                            _.each(configService.STATUS_PROCESSES_KEYS,
                                function(key) {
                                    ref_obj[key] = process[key];
                                });

                            return ref_obj;
                        };

                        var findChildren = function(process) {

                            return _.chain(status_all.processes)
                                .filter(function(sa_process) {
                                    return process.pid == sa_process.parent_pid
                                })
                                .map(function(sa_process) { return createRefObj(sa_process) })
                                .value();
                        };

                        // create nested references for tree view menu
                        status_processes.length = 0;
                        status_processes.push(createRefObj(
                            _.findWhere(status_all.processes, { "is_master": true })
                        ));

                        $timeout(poller, configService.UPDATE_DELTA);
                        status_all.calls++;
                    });
                });
        };

        return {
            "poller": poller,
            "status_current": status_current,
            "status_all": status_all,
            "status_processes": status_processes,
            getProcess: function(pid) {
                return _.find(status_all.processes, function(proc) { return proc.pid == pid });
            }
        };
    })

    // gets detailed status data
    .factory('processService', function($http, $q) {
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

    // gets basic status data
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


