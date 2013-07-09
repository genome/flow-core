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

    .factory('statusService', function ($http, $timeout, configService, basicService) {

        /*
        * MODEL VARIABLES
         */

        // response from the server
//        var service_data = {
//            "config": {},
//            "status": '',
//            "data": {}
//        };

        var service_data = { };

        // the current initialized status, processes flat
        var status_current = { };

        // all initialized status updates with histories, processes nested
        var status_all = {
            "calls": 0,
            "master_parent_pid": Number(),
            "master_pid": Number(),
            "processes": []
        };

        // these process attributes will be stored for historical displays
        var process_history_keys = [
            "memory_percent",
            "memory_vms",
            "cpu_user",
            "cpu_system",
            "memory_rss",
            "cpu_percent"
        ];

        // these file attributes will be stored for historical displays
        var file_history_keys = [
            "pos",
            "size"
        ];

        /*
         * PIPELINE FUNCTIONS
         */

        // initializes status_all
        var initStatusAll = function(stat_all, serv_data) {
            var processes = serv_data;
            basicService.get()
                .then(function(data) {
                    stat_all.master_pid = data.pid;
                    stat_all.master_parent_pid = data.parent_pid;
                    var master_object = _.findWhere(processes, { "pid": stat_all.master_pid });

                    master_object.name = ["Process", master_object.pid].join(" ");
                    master_object.is_running = true;
                    master_object.is_master = true;
                    master_object.children = [];

                    stat_all.processes.push(master_object);
                });
        }

        // ensure that initStatus is only called once
        var initStatusAllOnce = _.once(initStatusAll);

        // set is_running and is_master flags on a processes
        var setBooleans = function(proc) {
            proc.is_running = true;
            proc.is_master = (proc.pid === status_all.master_pid);
            return proc;
        };

        // initialize a processes node
        var initProcesses= function(proc) {
            // console.log("storeProcessHistory called.");
            return proc;
        };

        // initialize process file node
        var initFileData = function(proc) {
            // for each open_files, convert its fd to an array of objects
            proc.files = _.map(proc.open_files,
                function(open_file, key) {
                    var ofile = {};
                    ofile.name = key;
                    ofile.fd = convertObjectsToArray(open_file[key], 'id');
                    return ofile;
                });

            // convert all open_files from hash objects to an array of objects

//            proc.files = _.map(file_array,
//                function(fobj, fname){
//                    // console.log(["file:", fname].join(" "));
//                    // console.log(fobj);
//
//                    var fd = [];
//                    fd = _.map(fobj,
//                        function(fd, fdname) {
//                            return {
//                                "id": fdname,
//                                "finfo": fd
//                            }
//                        });
//
//                    return {
//                        "name": fname,
//                        "fd": fd
//                    }
//                });

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

        // convert an object hash into an array of objects, each with a new 'name' field copied from its key (not recursive)
        var convertObjectsToArray = function(obj_hash, name_field) {
            if (!_.isObject(obj_hash)) { return undefined; }
            return _.map(obj_hash,
                function(obj, key) {
                    if (obj.hasOwnProperty(name_field)) { console.warn(["Overwriting existing attribute, ", name_field, "containing", obj[name_field] ,"with", key].join(" ")); }
                    obj[name_field] = key;
                    return obj;
                });
        }


        /*
         * UTILITIES
         */

        var cloneObj = function(obj) {
            return JSON.parse(JSON.stringify(obj));
        }

        var cloneObj2 = function(obj) {
            if (obj === null || typeof obj !== 'object') {
                return obj;
            }

            var temp = obj.constructor(); // give temp the original obj's constructor
            for (var key in obj) {
                temp[key] = cloneObj2(obj[key]);
            }

            return temp;
        }

        /*
         * PIPELINES
         */

        // pipeline to initialize a service_data.data object to a status_current.processes object
        var initProcessPipeline = _.pipeline(
            setBooleans,
            initProcesses,
            initFileData,
            initProcessHistory,
            initFileDataHistory
        );


        // pipeline to merge a status_current process node with a status_all node

        // pipeline to update process and file bools in status_all

        /*
         * MAIN FUNCTIONS
         */

        // using AngularJS' $timeout, polls the /status REST service, updates status models w/ the response
        var poller = function(service_data) {
            service_data = {};
            $http.get('/status').then(
                function(r) { // success
                    service_data = _.deepExtend(service_data, r); // this is here mainly to get service_data to update in the main controller so we can see it

                    var processes = cloneObj(r.data);

                    initStatusAllOnce(status_all, processes);

                    status_current.processes = _.map(processes,
                        function(process) {
                            var proc = {};
                            proc = _.deepExtend(proc, process);
                            initProcessPipeline(proc);
                            return proc;
                        });

                    $timeout(poller, configService.UPDATE_DELTA);
                },
                function(r) { // failure
                    alert("Could not retrieve process status. " +
                        "The process has most likely completed. " +
                        "This monitor will continue to function but it will not receive any more updates.");
                }
            );

            status_all.calls++;
        };

        poller(service_data);

        return {
            "service_data": service_data,
            "status_current": status_current,
            "status_all": status_all
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


