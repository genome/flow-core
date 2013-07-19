angular.module('processMonitor.controllers', ['processMonitor.services', 'angularTree'])
    .controller('MainController', ['$scope', 'statusService',
        function($scope, statusService) {
            // console.log("MainController instantiated.");
            //$scope.status_current = statusService.status_current;
            //$scope.status_all = statusService.status_all;
            $scope.status_processes = statusService.status_processes;
        }])
    .controller('ProcessTree', ['$scope', 'statusService',
        function($scope, statusService) {
            $scope.processes = statusService.status_processes;

        }])
    .controller('ProcessTree2', ['$scope', 'statusService',
        function($scope, statusService) {
            var status_all = statusService.status_all;

            $scope.processes = statusService.status_processes;
            $scope.colors = {
                true:'#99CCFF',
                false:'white'
            };

            $scope.selection_class = {

            };

            $scope.getAttribute = function(pid, attribute) {
                var process = _.findWhere(status_all.processes, { "pid": pid});

                if (_.has(process, attribute)) {
                    return process[attribute];
                }
            };

            $scope.get10 = function() {
                return 10;
            }

            $scope.selected = function () {
                console.log(this.item.pid + ' is selected: ' + this.$selected);
                if (this.$selected) {
                    $scope.selected_pid = this.item.pid;
                }
            };
        }])
    .controller('BasicData', ['$scope', 'statusService',
        function($scope, statusService) {
            $scope.status_all = statusService.status_all;
        }]);