'use strict';

var Promise = require('bluebird');
var BunnyPaws = require('../lib/bunnyPaws.js');
var url = require('url');
var request = require('request');

module.exports = function (app, baseUrl, connectionUrl, amqpHttpApiPort, amqpHttpApiProtocol) {

    var bunnyPaws = new BunnyPaws(connectionUrl);
    var contextPath = url.parse(baseUrl + '/').pathname;
    var amqpBaseUrl;
    var amqpVhost;

    (function () {
        var parsed = url.parse(connectionUrl);
        amqpVhost = parsed.pathname || '/';
        if (amqpHttpApiPort) {
            parsed.port = amqpHttpApiPort;
            parsed.host = parsed.hostname + ':' + amqpHttpApiPort;
        }
        parsed.protocol = amqpHttpApiProtocol || 'http:';
        parsed.pathname = '/api';
        amqpBaseUrl = url.format(parsed);
    })();

    function getLocalUrl(queueName) {
        return url.resolve(baseUrl + '/', './queues/' + (queueName || ''));
    }

    function getAmqpHttpApiUrl(queueName) {
        return url.resolve(amqpBaseUrl + '/', './queues/' + encodeURIComponent(amqpVhost) + '/' + (queueName || ''));
    }

    function pauseResume(resume) {
        var method = resume ? 'resume' : 'pause';
        return function (req, res) {
            bunnyPaws[method](req.params.queueName).then(function () {
                var url = getLocalUrl(req.params.queueName);
                res.set('Location', url).send({metrics: url}).status(204);
            }).catch(function (error) {
                    console.error(error && error.stack || error);
                    res.sendStatus(500);
                });
        };
    }

    function createPauseHandler() {
        return pauseResume();
    }

    function createResumeHandler() {
        return pauseResume(true);
    }

    function queueInfo(req, res) {
        var queueName = req.params.queueName || '';
        try {
            var theUrl = getAmqpHttpApiUrl(queueName);
            request(theUrl).pipe(res);
        } catch (e) {
            console.error(e && e.stack || e);
            res.sendStatus(500);
        }
    }

    app.post(contextPath + 'queues/:queueName/pause', createPauseHandler());
    app.post(contextPath + 'queues/:queueName/resume', createResumeHandler());
    app.post(contextPath + 'queues/pause', createPauseHandler());
    app.post(contextPath + 'queues/resume', createResumeHandler());
    app.get(contextPath + 'queues', queueInfo);
    app.get(contextPath + 'queues/:queueName', queueInfo);

    return Promise.resolve(bunnyPaws);
};
