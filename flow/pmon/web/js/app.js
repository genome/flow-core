'use strict';
var processMonitorApp = angular.module('processMonitor', ['processMonitor.controllers', 'processMonitor.services', 'processMonitor.directives', 'processMonitor.filters'])
    .config(['$routeProvider', function($routeProvider, $location, configService, ProcessDetail) {
        // underscore.js extensions
        _.mixin({
            deepExtend: deepExtend, // basic merging for nested objects/arrays
            mergeProcess: mergeProcess // custom function for merging whole processes
        });

        $routeProvider
            .when('/process/:pid', {
                controller: ProcessDetail,
                templateUrl: 'templates/process-detail.html'
            });

        console.log("processMonitor configured.");
    }])

    .run(['$location', '$rootScope', 'statusService', 'basicService', 'configService', function($location, $rootScope, statusService, basicService, configService) {
        $rootScope.host = $location.host();
        $rootScope.version = configService.VERSION;
        $rootScope.app_name= configService.APP_NAME;
        $rootScope.$on('$routeChangeSuccess', function (event, current, previous) {
            $rootScope.host = $location.host();
        });

        // set the master ids then call the poller
        basicService.get()
            .then(function(data) {
                statusService.status_all.master_pid = data.pid;
                statusService.status_all.master_parent_pid = data.parent_pid;
            })
            .then(function(){
                statusService.poller();
            })
            .then(function() {
                // set path to view master PID if URL doesn't exist
                if($location.path() == "") {
                    $location.path("process/" + statusService.status_all.master_pid);
                    $rootScope.pid = statusService.status_all.master_pid;
                    $rootScope.title = "Flow " + $rootScope.host + ":" + $rootScope.pid;
                } else {
                    $rootScope.pid = statusService.status_all.master_pid;
                    $rootScope.title = "Flow " + $rootScope.host + ":" + $rootScope.pid;
                }

            });

        console.log("processMonitor run.");
    }]);
