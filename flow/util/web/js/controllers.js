angular.module('processMonitor.controllers', ['processMonitor.services', 'processMonitor.directives'])
    .controller('Tree', ['$scope', '$location', '$rootScope', 'statusService',
        function($scope, $location, $rootScope, statusService) {
            $scope.processes = statusService.status_processes;

            $scope.selected = null;
            $scope.hover = null;

            $scope.viewProcess= function (item) {
                $location.path("process/" + item.pid);
                $scope.selected = item.pid;
                $rootScope.pid = item.pid;
                $rootScope.title = "Flow " + $rootScope.host + ":" + $rootScope.pid;
            };

            $scope.mouseEnter = function(item) {
                $scope.hover = item.pid;
            };

            $scope.mouseLeave = function(item) {
                $scope.hover = null;
            };

            $scope.tabClass = function(item) {
                if ((item.pid === $scope.hover) && (item.pid === $scope.selected)) {
                    return "hover selected";
                } else if (item.pid === $scope.hover) {
                    return "hover";
                } else if (item.pid === $scope.selected) {
                    return "selected";
                } else {
                    return undefined;
                }
            };


        }])

    .controller('BasicData', ['$scope', 'statusService',
        function($scope, statusService) {
            $scope.status_all = statusService.status_all;
        }])

    .controller('ProcessDetail', ['$scope', '$routeParams', '$q', 'statusService',
        function($scope, $routeParams, $q, statusService){
            var pid = $routeParams['pid'];

            $scope.assignProcessData = function() {
                console.log(['Assigning process_data from process', pid].join(" "));
                $scope.process_data = getProcessData(pid);
                $scope.process_id = pid;
                $scope.process_data.then(function(pdata) {
                    $scope.command_line = formatCommandLine(pdata.cmdline);
                });
            };

            var getProcessData = function (pid) {
                var deferred = $q.defer();
                var process_data = statusService.getProcess(pid);
                if (_.isObject(process_data)) {
                    deferred.resolve(process_data);
                } else {
                    setTimeout(function() {
                        $scope.$apply(function() {
                            var process_data = statusService.getProcess(pid);
                            if (_.isObject(process_data)) {
                                deferred.resolve(process_data);
                            } else {
                                deferred.reject('Could not load process_data for detail view.');
                            }
                        });
                    }, 1000);
                }
                return deferred.promise;
            };

            var formatCommandLine = function(cmdline) {
                var shortCmdLine = _.map(cmdline, function(part) {
                    if ((part.match(/\//g)||[]).length > 1) { // find long pathnames, only return the final file string
                        return part.split("/").pop();
                    } else {
                        return part;
                    }
                });
                return shortCmdLine.join(" ");
            }
        }]);