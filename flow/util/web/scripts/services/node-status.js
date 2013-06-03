var services = angular.module('flow.services', ['ngResource']);

services.factory('NodeStatus', ['$resource', function($resource) {
    return $resource('/basic/:id', {id: '@id'});
}]);
