angular.module('processMonitor.controllers', ['processMonitor.services'])
    .controller('MainController', ['$scope', '$timeout', 'configService',
        function($scope, $timeout, configService) {
            console.log("MainController instantiated.");
            console.log("configService " + configService);
            $scope.update_delta = configService.update_delta;
        }])
    .controller('ProcessTree', ['$scope', '$timeout',
        function($scope, $timeout) {
            console.log("ProcessTree instantiated.");

        }])
    .controller('CpuUpdate', ['$scope', '$timeout',
        function($scope, $timeout){
            console.log("CpuUpdate instantiated.");

        }]);
