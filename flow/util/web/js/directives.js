angular.module('processMonitor.directives', [])
    .directive('cpuChart', function () {
        return {
            restrict: 'E',
            templateUrl: 'templates/directives/cpu-chart.html',
            require: "^ngModel",
            transclude: true,
            replace: true,
            scope: {
                process_data: "=ngModel"
            },

            controller: ['$scope', function($scope) {
                $scope.buildChart = function(element, data, options) {
                    // remove old chart
                    if (!d3.select("svg").empty()) {
                        d3.select("svg").remove();
                    }
                    var margin = {
                        top: options.top || 20,
                        right: options.right || 10,
                        bottom: options.bottom || 20,
                        left: options.left || 10
                    };

                    var width = (options.width || 780) - margin.left - margin.right,
                        height = (options.height || 350) - margin.top - margin.bottom;

                    var svg = d3.select(element[0])
                        .append('svg:svg')
                        .attr('class', 'line-chart')
                        .attr("width", width + margin.left + margin.right)
                        .attr("height", height + margin.top + margin.bottom)
                        .append("svg:g")
                        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

                    svg.selectAll('*').remove();

                    var xData = _.findWhere(data, {"name": options.xVar}).values;
                    var yData = _.findWhere(data, {"name": options.yVar}).values;

                    var chartData = _.zip(xData, yData);

                    var maxX = d3.max(xData),
                        maxY = d3.max(yData),
                        minX = d3.min(xData),

                        x = d3.scale.linear()
                            .domain([minX, maxX])
                            .range([0, width]),
                        y = d3.scale.linear()
                            .domain([0, maxY])
                            .range([height, 0]),
                        yAxis = d3.svg.axis().scale(y)
                            .orient('left')
                            .ticks(5),
                        xAxis = d3.svg.axis().scale(x)
                            .orient('bottom')
                            .ticks(10);

                    svg.append('svg:g')
                        .attr('class', 'y-axis')
                        .call(yAxis);

                    svg.append('svg:g')
                        .attr('class', 'x-axis')
                        .attr("transform", "translate(0," + height + ")")
                        .call(xAxis);

                    var line = d3.svg.line()
                        .x(function(d,i){ return x(d[0]); })
                        .y(function(d,i){ return y(d[1]); })
                        .interpolate('linear');

                    svg.append('svg:path')
                        .attr('d', line(chartData))
                        .attr('class', 'chart-line')
                        .attr('fill', 'none')
                        .attr('stroke-width', '1');
                };
            }],

            link: function(scope, iElement, iAttributes) {
                scope.$watch('process_data', function(process_data) {
                    var process_history = process_data.history;
                    scope.process_history = process_history; // for testing
                    if(process_history) {
                        // build chart
                        var data = [
                            {
                                "name": "index",
                                "values": _.pluck(process_history, "index")
                            },
                            {
                                "name": "time",
                                "values": _.pluck(process_history, "time")
                            },
                            {
                                "name": "cpu_user",
                                "values": _.pluck(process_history, "cpu_user")
                            },
                            {
                                "name": "cpu_percent",
                                "values": _.pluck(process_history, "cpu_percent")
                            },
                            {
                                "name": "cpu_system",
                                "values": _.pluck(process_history, "cpu_system")
                            }
                        ];

                        var options = {
                            "height": iAttributes.height,
                            "width": iAttributes.width,
                            "top": 0,
                            "right": 0,
                            "left": 35,
                            "bottom": 25,
                            "xVar": "index",
                            "yVar": "cpu_percent"
                        };

                        scope.buildChart(iElement, data, options);
                    }
                }, true);
            }
        };
    })

    .directive('fileList', function() {
        return {
            restrict: 'E',
            templateUrl: 'templates/directives/file-list.html',
            replace: true,
            scope: true,

            controller: ['$scope', function($scope) {
                console.log('FileList controller called.');
                $scope.buildFileList = function(element, data, options) {
                   // console.log('buildFileList called.');
                }
            }],

            link: function(scope, iElement, iAttributes) {
                console.log("fileList directive linked.");
                scope.$watch('process_data', function(process_data) {
                         scope.buildFileList(iElement);
                }, true);

            }
        };
    });