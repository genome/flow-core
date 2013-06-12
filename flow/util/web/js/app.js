'use strict';
console.log("app.js loaded");

angular.module('processMonitor', ['processMonitor.controllers','processMonitor.services', 'angularTree'])
    .config([function() {
        console.log("processMonitor configured.");
    }])
    .run(function(configService, statusService, basicService) {
        console.log("processMonitor run.");
        var basicNode = basicService.getBasic();
        configService.MASTER_PID = basicNode.pid;
        console.log("basicNode =-=-=-=-=-=-=-=-");
        console.log(basicNode);
    });
