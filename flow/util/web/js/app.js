'use strict';
console.log("app.js loaded");

angular.module('processMonitor', ['processMonitor.controllers','processMonitor.services', 'angularTree'])
    .config([function() {
        // extend underscore.js
        _.mixin({
            deepExtend: deepExtend, // add deepExtend mixin to underscore.js
            merge: function() {
                var objects = arguments;
                return _.reduce(_.rest(objects), function(obj, o) {
                    return _.extend(obj, o);
                }, _.clone(_.first(objects) || {}));
            },
            deepClone: function (p_object) { return JSON.parse(JSON.stringify(p_object)); }
        });


        console.log("processMonitor configured.");
    }])
    .run(function(configService, statusService, basicService) {
        console.log("processMonitor run.");
    });
