'use strict';

var Promise = require('bluebird');
var Libarka = require('../lib/libarka.js');
var url = require('url');
var request = require('request');

module.exports = function (app, connectionUrl, baseUrl, amqpBaseUrl, ampqVhost) {
    var libarka = new Libarka(connectionUrl);

    var contextPath = url.parse(baseUrl + '/').pathname;

    function getUrl(queueName) {
        return url.resolve(baseUrl + '/', './queues/' + encodeURIComponent(ampqVhost) + '/' + (queueName || ''));
    }

    function pauseResume(resume) {
        var method = resume ? 'resume' : 'pause';
        return function (req, res) {
            libarka[method](req.params.queueName).then(function () {
                var url = getUrl(req.params.queueName);
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
        var ampqVhost = req.params.vhost;
        var queueName = req.params.queueName || '';
        var theUrl = url.resolve(amqpBaseUrl + '/', './queues/' + encodeURIComponent(ampqVhost) + '/' + (queueName || ''));
        request(theUrl).pipe(res);
    }

    app.post(contextPath + 'queues/:queueName/pause', createPauseHandler());
    app.post(contextPath + 'queues/:queueName/resume', createResumeHandler());
    app.post(contextPath + 'queues/pause', createPauseHandler());
    app.post(contextPath + 'queues/resume', createResumeHandler());
    app.get(contextPath + 'queues/:vhost', queueInfo);
    app.get(contextPath + 'queues/:vhost/:queueName', queueInfo);

    return Promise.resolve(libarka);
};
