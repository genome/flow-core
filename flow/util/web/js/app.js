'use strict';
angular.module('processMonitor', ['processMonitor.controllers', 'processMonitor.services', 'processMonitor.directives', 'processMonitor.filters'])
    .config(['$routeProvider', function($routeProvider, ProcessDetail) {
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
    .run(function(statusService, basicService) {
        // set the master ids then call the poller
        basicService.get()
            .then(function(data) {
                statusService.status_all.master_pid = data.pid;
                statusService.status_all.master_parent_pid = data.parent_pid;
            })
            .then(function(){
                statusService.poller();
            });
        console.log("processMonitor run.");
    });
