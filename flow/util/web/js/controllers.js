angular.module('processMonitor.controllers', ['processMonitor.services', 'processMonitor.directives', 'angularTree'])
    .controller('ProcessTree', ['$scope', 'statusService',
        function($scope, statusService) {
            $scope.processes = statusService.status_processes;
            $scope.colors = {
                true:'#99CCFF',
                false:'white'
            };

            $scope.selection_class = {

            };

            $scope.getData = function (pid) {
                console.log(["Selected", pid].join(" "));
//                if (this.$selected) {
//                    $scope.selected_pid = this.item.pid;
//                }
            };
        }])

    .controller('Tree', ['$scope', '$location', 'statusService',
        function($scope, $location, statusService) {
            console.log("Tree controller instantiated.");
            $scope.processes = statusService.status_processes;

            $scope.viewProcess= function (pid) {
                console.log(["Selected", pid].join(" "));
                $location.path("process/" + pid);
            };
        }])

    .controller('BasicData', ['$scope', 'statusService',
        function($scope, statusService) {
            $scope.status_all = statusService.status_all;
        }])

    .controller('ProcessDetail', ['$scope', '$routeParams', 'statusService',
        function($scope, $routeParams, statusService){
            var pid = $routeParams['pid'];

            $scope.assignProcessData = function() {
                console.log(['Assigning process_data from process', pid].join(" "));
                $scope.process_data = statusService.getProcess(pid);
                $scope.process_id = pid;
            };

//            var updateProcess = function(pid) {
//                console.log(["Updating process", pid].join(" "));
//                $scope.process_detail = statusService.getProcess(pid);
//            };
//
//            $scope.addWatcher = function() {
//                $scope.$watch(statusService.getProcess(pid)['history'].length, updateProcess(pid));
//            }

        }]);