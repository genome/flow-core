angular.module('processMonitor.filters', [])
    .filter('longFileName', function () {
        return function (input) {
            console.log("longFileName filter called.");
            var fileParts = input.split("/");
            var includedParts = fileParts.slice(Math.max(fileParts.length - 3, 1));

            includedParts.unshift("...");

            return includedParts.join("/");
        }

    });
