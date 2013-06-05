PROCESS_TREE_UPDATE_DELAY = 1000;
CPU_UPDATE_DELAY = 300;


var processMonitorModule = angular.module('processMonitorApp', ['angularTree']);


processMonitorModule.controller('MainController',
    function($scope, $timeout) {
        function tick() {
            $scope.master_pid = master_pid;
            $scope.process_status = process_status;
            $timeout(tick, 10000);
        }
        tick();
    }
);

processMonitorModule.controller('CpuUpdate',
    function($scope, $timeout) {
        function tick() {
            $scope.cpu_percent = update_cpu_percent($scope.process_status);
            $timeout(tick, CPU_UPDATE_DELAY);
        }

        function update_cpu_percent(ps) {
            result = {};
            for (var pid in $scope.process_status) {
                var psp = ps[pid];
                var cpu = '-';
                if (psp.is_running) {
                    var len = psp['cpu_percent']['values'].length - 1;
                    var cpu = psp['cpu_percent']['values'][len]['y'];
                }
                result[pid] = cpu;
            }
            return result;
        }
        tick();
    }
);

processMonitorModule.controller('ProcessTree',
    function($scope, $timeout) {
        $scope.selected = function () {
           console.log(this.item.pid + ' is selected: ' + this.$selected);
           if (this.$selected) {
               $scope.selected_pid = this.item.pid;
           }
        };
        $scope.colors = {
            true:'#99CCFF',
            false:'white',
        };

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
