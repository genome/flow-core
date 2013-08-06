angular.module('processMonitor.directives', [])
    .directive('cpuChart', function () {
        return {
            restrict: 'A',
            templateUrl: 'templates/directives/cpu-chart.html',
            require: "^ngModel",
            scope: {
                process_data: "=ngModel"
            },
            controller: ['$scope', function($scope) {
                // create data structure for cpu_user chart from process_data.history
                console.log("cpuChart directive controller instantiated.");
            }],
            link: function(scope, element, attributes) {
                // pluck cpu history for d3 chart
                var width = attributes.width;
                var height = attributes.height;
                console.log("cpuChart directive linked.");
                scope.$watch('process_data', function(process_data) {
                    console.log("$watch cpu_history fired.");
                    var process_history = process_data.history;
                    if(process_history) {
                        console.log("process history: ");
                        console.log(process_history);
                    }
                }, true);
            }

        };
    });