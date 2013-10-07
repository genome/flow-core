mergeProcess = function(obj) {
    var parentRE = /#{\s*?_\s*?}/,
        hasOwnProperty = Object.prototype.hasOwnProperty;

    _.each(_.rest(arguments), function(source) {
        for (var prop in source) {
            if (_.isUndefined(obj[prop]) || _.isFunction(obj[prop]) || _.isNull(source[prop])) {
                obj[prop] = source[prop];
            }
            else if (_.isString(source[prop]) && parentRE.test(source[prop])) {
                if (_.isString(obj[prop])) {
                    obj[prop] = source[prop].replace(parentRE, obj[prop]);
                }
            }
            else if (_.isArray(obj[prop]) || _.isArray(source[prop])){
                if (!_.isArray(obj[prop]) || !_.isArray(source[prop])){
                    throw 'Error: Trying to combine an array with a non-array (' + prop + ')';
                } else if (prop == "history") {
                    obj[prop].push(source[prop][0]);
                } else if (prop == "files") {
                    _.each(source[prop], function(file) {
                        _.mergeProcess(file, _.findWhere(obj[prop], {"name": file.name}));
                    });
                } else if (prop == "descriptors") {
                    _.each(source[prop], function(descriptor) {
                        _.mergeProcess(descriptor, _.findWhere(obj[prop], {"id": descriptor.id}));
                    });
                } else {
                    obj[prop] = _.reject(_.deepExtend(obj[prop], source[prop]), function (item) { return _.isNull(item);});
                }
            }
            else if (_.isObject(obj[prop]) || _.isObject(source[prop])){
                if (!_.isObject(obj[prop]) || !_.isObject(source[prop])){
                    throw 'Error: Trying to combine an object with a non-object (' + prop + ')';
                } else {
                    obj[prop] = _.deepExtend(obj[prop], source[prop]);
                }
            } else {
                obj[prop] = source[prop];
            }
        }

    });
    return obj;
};
