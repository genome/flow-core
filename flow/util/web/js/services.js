angular.module('processMonitor.services', [])
    .factory('configService', function() {
        // holds important app parameters
        console.log("configService instantiated.");

        return {
            process_tree_update_delay: 1000,
            cpu_update_delay: 300,
            update_delta: 10000,
            num_data_pts: 1000
        };

    })
    .factory('processBasicService', function($http) {
        // fetches the /basic process data node once
        var basic = { response: {} };
        $http.get('/basic').then(function(r) {
            basic.response = r.data;
        });

        return {
            basic: basic
        };
    })
    .factory('processStatusService', function($http, $timeout) {
        // polls and updates the /status process data node
        var status = { response: {}, calls: 0 };
        var poller = function() {
            $http.get('/status').then(function(r) {
                status.response = r.data;
                $timeout(poller, 1000);
            });
        };
        poller();

        return {
            status: status
        };
    });
