angular.module('processMonitor.controllers', ['processMonitor.services'])
    .controller('MainController', ['$scope', '$timeout', 'configService', 'statusService',
        function($scope, $timeout, configService, statusService) {
            console.log("MainController instantiated.");
            // $scope.update_delta = configService.update_delta;
            $scope.currentStatus = statusService.currentStatus;
            $scope.all_status = statusService.all_status;
            $scope.current_status = statusService.current_status;
            $scope.all_status_nested = statusService.all_status_nested;

            $scope.UPDATE_DELTA = statusService.UPDATE_DELTA;

        }])
    .controller('ProcessTree', ['$scope', '$timeout',
        function($scope, $timeout) {
            console.log("ProcessTree instantiated.");

        }])
    .controller('CpuUpdate', ['$scope', '$timeout',
        function($scope, $timeout){
            console.log("CpuUpdate instantiated.");

        }]);
