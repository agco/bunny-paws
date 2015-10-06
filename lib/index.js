'use strict';

var BunnyPaws = require('./bunnyPaws.js');
module.exports = {
    newInstance: function (onnectionUrl, config) {
        return new BunnyPaws(onnectionUrl, config);
    },
    registerRoutes: require('./rest.js')
};
