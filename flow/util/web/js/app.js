 'use strict';
console.log("app.js loaded");

angular.module('processMonitor', ['processMonitor.controllers','processMonitor.services', 'angularTree'])
    .config([function() {
        // extend underscore.js
        _.mixin({
            deepExtend: deepExtend // add deepExtend mixin to underscore.js
        });


        // console.log("processMonitor configured.");
    }])
    .run(function(configService, statusService, basicService) {
        // console.log("processMonitor run.");
    });
