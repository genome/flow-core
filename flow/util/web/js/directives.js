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
                    var y0Data = _.findWhere(data, {"name": options.yVar0}).values;
                    var y1Data = _.findWhere(data, {"name": options.yVar1}).values;

                    var y0ChartData = _.zip(xData, y0Data);
                    var y1ChartData = _.zip(xData, y1Data);

                    var maxX = d3.max(xData),
                        maxY0 = d3.max(y0Data),
                        maxY1 = d3.max(y1Data),
                        minX = d3.min(xData);

                    if (options.xVar == "time") {
                        var x = d3.time.scale()
                            .domain([new Date(minX * 1000), new Date(maxX * 1000)])
                            .range([0, width]);
                    } else {
                        var x = d3.scale.linear()
                            .domain([minX, maxX])
                            .range([0, width]);
                    }

                    var y0 = d3.scale.linear()
                        .domain([0, maxY0])
                        .range([height, 0]);

                    var y1 = d3.scale.linear()
                        .domain([0, maxY1])
                        .range([height, 0]);

                    var yAxis0 = d3.svg.axis().scale(y0)
                        .orient('left')
                        .ticks(5);

                    var yAxis1 = d3.svg.axis().scale(y1)
                        .orient('right')
                        .ticks(5);

                    var xAxis = d3.svg.axis().scale(x)
                        .orient('bottom')
                        .ticks(10);

                    svg.append('svg:g')
                        .attr('class', 'y-axis')
                        .call(yAxis0);

                    svg.append('svg:g')
                        .attr('class', 'y-axis')
                        .attr("transform", "translate(" + width + ",0)")
                        .call(yAxis1);

                    // y0 axis label
                    svg.append("svg:text")
                        .attr("class", "y label")
                        .attr("text-anchor", "middle")
                        .attr("y", -(options.left - 15))
                        .attr("x", -(height/2))
                        .attr("transform", "rotate(-90)")
                        .text(options.yVar0Label);

                    // y1 axis label
                    svg.append("svg:text")
                        .attr("class", "y label")
                        .attr("text-anchor", "middle")
                        .attr("y", -(options.right))
                        .attr("x", -(height/2))
                        .attr("transform", "rotate(90)")
                        .text(options.yVar1Label);

                    // x axis label
                    svg.append("svg:text")
                        .attr("class", "x label")
                        .attr("text-anchor", "end")
                        .attr("x", width/2)
                        .attr("y", height + 32)
                        .text(options.xVar);

                    svg.append('svg:g')
                        .attr('class', 'x-axis')
                        .attr("transform", "translate(0," + height + ")")
                        .call(xAxis);

                    var line = d3.svg.line()
                        .x(function(d,i){
                            if (options.xVar == "time") {
                                return x(new Date(d[0] * 1000));
                            } else {
                                return x(d[0]);
                            }
                        })
                        .y(function(d,i){ return y0(d[1]); })
                        .interpolate('linear');

                    svg.append('svg:path')
                        .attr('d', line(y0ChartData))
                        .attr('class', 'chart-line')
                        .attr('fill', 'none')
                        .attr('stroke-width', '1');

                    svg.append('svg:path')
                        .attr('d', line(y1ChartData))
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
                            },
                            {
                                "name": "memory_percent",
                                "values": _.pluck(process_history, "memory_percent")
                            }
                        ];

                        var options = {
                            "height": iAttributes.height,
                            "width": iAttributes.width,
                            "top": 0,
                            "right": 45,
                            "left": 45,
                            "bottom": 40,
                            "xVar": "time",
                            "yVar0": "cpu_percent",
                            "yVar0Label": "CPU %",
                            "yVar1": "memory_percent",
                            "yVar1Label": "Memory %"
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