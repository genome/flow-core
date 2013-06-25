angular.module('processMonitor.controllers', ['processMonitor.services'])
    .controller('MainController', ['$scope', '$timeout', 'configService', 'statusService',
        function($scope, $timeout, configService, statusService) {
            // console.log("MainController instantiated.");
            $scope.status_current = statusService.status_current;
            $scope.status_all= statusService.status_all;
            $scope.status_all_nested= statusService.status_all_nested;


        }])
    .controller('ProcessTree', ['$scope', '$timeout',
        function($scope, $timeout) {
            // console.log("ProcessTree instantiated.");

        }])
    .controller('CpuUpdate', ['$scope', '$timeout',
        function($scope, $timeout){
            // console.log("CpuUpdate instantiated.");

        }]);
