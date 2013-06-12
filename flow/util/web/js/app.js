'use strict';
console.log("app.js loaded");

angular.module('processMonitor', ['processMonitor.controllers','processMonitor.services', 'angularTree'])
    .config([function() {
        console.log("processMonitor configured.");
    }])
    .run(function(processStatusService, getBasicService) {
        console.log("processMonitor run.")
    });
