'use strict';

var express = require('express');
var config = require('./config.js');
var libarka = require('../lib/libarka.js')();

var app = express();

libarka.connect();

function getUrl(queueName) {
    return config.amqp.httpApiBaseUrl + '/api/queues/' + encodeURIComponent(config.amqp.vhost) + '/' + (queueName || '');
}

function pause(req, res) {
    libarka.pause(req.params.queueName).then(function () {
        var url = getUrl(req.params.queueName);
        res.set('Location', url).send({metrics: url}).status(204);
    }).catch(function (error) {
            console.error(error && error.stack || error);
            res.sendStatus(500);
        });
}

function resume(req, res) {
    libarka.resume(req.params.queueName).then(function () {
        var url = getUrl(req.params.queueName);
        res.set('Location', url).status(204).send({metrics: url});
    }).catch(function (error) {
            console.error(error && error.stack || error);
            res.sendStatus(500);
        });
}

app.post('/queues/:queueName/pause', pause);
app.post('/queues/:queueName/resume', resume);
app.post('/queues/pause', pause);
app.post('/queues/resume', resume);

app.listen(config.port);

module.exports = app;
