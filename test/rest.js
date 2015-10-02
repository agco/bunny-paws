var express = require('express');
var libarkaModule = require('../lib/index.js');
var config = require('./config.js');

var app = express();

libarkaModule.registerRoutes(app, config.amqp.httpApiBaseUrl, config.amqp.vhost);

app.listen(config.port);
