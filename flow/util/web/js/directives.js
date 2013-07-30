angular.module('processMonitor.directives', [])
    .directive('cpuChart', function () {

        return {
            restrict: 'A',
            templateUrl: 'templates/directives/cpu-chart.html',
            require: "^ngModel",
            scope: {
                process_data: "=ngModel"
            }
        };
    });