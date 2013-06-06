angular.module('processMonitor.controllers', ['processMonitor.services'])
    .controller('MainController', ['$scope', '$timeout', 'configService', 'processStatusService', 'processBasicService',
        function($scope, $timeout, configService, processStatusService, processBasicService) {
            console.log("MainController instantiated.");
            $scope.update_delta = configService.update_delta;
            $scope.processBasic = processBasicService.basic;
            $scope.processStatus = processStatusService.status;
        }])
    .controller('ProcessTree', ['$scope', '$timeout',
        function($scope, $timeout) {
            console.log("ProcessTree instantiated.");

        }])
    .controller('CpuUpdate', ['$scope', '$timeout',
        function($scope, $timeout){
            console.log("CpuUpdate instantiated.");

        }]);
