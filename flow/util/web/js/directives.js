angular.module('processMonitor.directives', [])
    .directive("tree", function($compile) {
        return {
            restrict: "E",
            scope: { family: '=' },
            template:
            '<p>{{ family.id }}</p>'+
                '<ul>' +
                '<li ng-repeat="child in family.children">' +
                '<tree family="child"></tree>' +
                '</li>' +
                '</ul>',
            compile: function(tElement, tAttr) {
                console.log("tree directive compiling");
                var contents = tElement.contents().remove();
                var compiledContents;
                return function(scope, iElement, iAttr) {
                    if(!compiledContents) {
                        compiledContents = $compile(contents);
                    }
                    compiledContents(scope, function(clone, scope) {
                        iElement.append(clone);
                    });
                };
            }
        };
    })
    .directive("hello", function() {
        return {
            restrict: 'E',
            templateUrl: 'templates/process-tree.html',
            replace: true
        }
    });