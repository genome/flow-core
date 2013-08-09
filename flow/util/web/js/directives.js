angular.module('processMonitor.directives', [])
    .directive('cpuChart', function () {
        return {
            restrict: 'A',
            templateUrl: 'templates/directives/cpu-chart.html',
            require: "^ngModel",
            transclude: true,
            replace: true,
            scope: {
                process_data: "=ngModel"
            },

            controller: ['$scope', function($scope) {
                $scope.buildChart = function(element, data, options) {
                    console.log("buildChart called.");
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
                        .append("g")
                        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

                    svg.selectAll('*').remove();

                    var xData = _.findWhere(data, {"name": options.xVar}).values;
                    var yData = _.findWhere(data, {"name": options.yVar}).values;

                    var chartData = _.zip(xData, yData);

                    var maxX = d3.max(xData),
                        maxY = d3.max(yData),

                        x = d3.scale.linear()
                            .domain([0, maxX])
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

                    svg.append('g')
                        .attr('class', 'y-axis')
                        .call(yAxis);

                    svg.append('g')
                        .attr('class', 'x-axis')
                        .attr("transform", "translate(0," + height + ")")
                        .call(xAxis);

                    var line = d3.svg.line()
                        .x(function(d,i){
                            console.log("plotting x: " + x(d[0]));
                            return x(d[0]);
                        })
                        .y(function(d,i){
                            console.log("plotting y: " + y(d[1]));
                            return x(d[1]);
                        })
                        .interpolate('linear');

                    var path = svg.append('svg:path')
                        .attr('d', line(chartData))
                        .attr('fill', 'none')
                        .attr('stroke-width', '1');
                };
            }],

            link: function(scope, iElement, iAttributes) {

                console.log("cpuChart directive linked.");
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
                            "left": 40,
                            "bottom": 20,
                            "xVar": "index",
                            "yVar": "cpu_percent"
                        };

                        scope.buildChart(iElement, data, options);
                    }
                }, true);
            }
        };
    });