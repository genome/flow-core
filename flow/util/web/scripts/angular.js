PROCESS_TREE_UPDATE_DELAY = 1000;


var processMonitorModule = angular.module('processMonitorApp', ['angularTree']);


processMonitorModule.controller('MainController',
    function($scope, $timeout) {
        function tick() {
            $scope.master_pid = master_pid;
            $scope.process_status = process_status;
            $timeout(tick, 1000);
        }
        tick();
    }
);


processMonitorModule.controller('ProcessTree',
    function($scope, $timeout) {
        function tick() {
            $scope.process_tree = update_process_tree($scope.process_status);
            $timeout(tick, PROCESS_TREE_UPDATE_DELAY);
        }

        function update_process_tree(ps) {
            var nodes = {};
            for (var pid in ps) {
                var psp = ps[pid];
                nodes[pid] = {
                    'pid':psp.pid,
                    'children':[],
                }

                if (psp.is_running) {
                    var len = psp['cpu_percent']['values'].length - 1;
                    var cpu = psp['cpu_percent']['values'][len]['y'];
                    nodes[pid]['cpu'] = cpu;
                } else {
                    nodes[pid]['cpu'] = '-';
                }

                if ('cmdline' in psp) {
                    var cmdline = psp.cmdline.join(' ');
                    nodes[pid]['cmdline'] = cmdline;
                } else {
                    nodes[pid]['cmdline'] = 'unknown'
                }
            }

            for (var pid in nodes) {
                var ps = process_status[pid];
                var parent_pid = ps.parent_pid;
                if (parent_pid in nodes) {
                    nodes[ps.parent_pid]['children'].push(nodes[pid]);
                }
            }
            return [nodes[master_pid]];
        }

        tick();
    }
);


