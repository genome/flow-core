angular.module('processMonitor.filters', [])
    .filter('longFileName', function () {
        return function (input) {
            if (input.length > 30) {
                var fileParts = input.split("/");
                var includedParts = fileParts.slice(Math.max(fileParts.length - 3, 1));
                includedParts.unshift("...");
                return includedParts.join("/");
            } else {
                return input;
            }
        }
    });
