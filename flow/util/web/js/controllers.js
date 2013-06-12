angular.module('processMonitor.controllers', ['processMonitor.services'])
    .controller('MainController', ['$scope', '$timeout', 'configService', 'statusService', 'basicService',
        function($scope, $timeout, configService, processStatusService) {
            console.log("MainController instantiated.");
            $scope.update_delta = configService.update_delta;
            $scope.currentStatus = processStatusService.currentStatus;
            $scope.allStatus = processStatusService.allStatus;
        }])
    .controller('ProcessTree', ['$scope', '$timeout',
        function($scope, $timeout) {
            console.log("ProcessTree instantiated.");

        }])
    .controller('CpuUpdate', ['$scope', '$timeout',
        function($scope, $timeout){
            console.log("CpuUpdate instantiated.");

        }]);
