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
    })
    .filter('humanReadableBytes', function() {
        return function (bytes) {
            if( isNaN( bytes ) ){ return bytes; }
            var units = [ ' bytes', ' KB', ' MB', ' GB', ' TB', ' PB', ' EB', ' ZB', ' YB' ];
            var amountOf2s = Math.floor( Math.log( +bytes )/Math.log(2) );
            if( amountOf2s < 1 ){
                amountOf2s = 0;
            }
            var i = Math.floor( amountOf2s / 10 );
            bytes = +bytes / Math.pow( 2, 10*i );

            // Rounds to 3 decimals places.
            if( bytes.toString().length > bytes.toFixed(3).toString().length ){
                bytes = bytes.toFixed(3);
            }
            return bytes + units[i];
        }
    });
