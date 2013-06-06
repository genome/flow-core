angular.module('processMonitor.services', [])
    .factory('configService', function() {
        return {
            process_tree_update_delay: 1000,
            cpu_update_delay: 300,
            update_delta: 10000,
            num_data_pts: 1000
        };
    });
