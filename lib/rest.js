'use strict';

var Promise = require('bluebird');
var Libarka = require('../lib/libarka.js');

module.exports = function (app, connectionUrl, amqpHttpApiBaseUrl, ampqVhost) {
    var libarka = new Libarka(connectionUrl);

    function getUrl(queueName) {
        return amqpHttpApiBaseUrl + '/api/queues/' + encodeURIComponent(ampqVhost) + '/' + (queueName || '');
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
        }
    }

    function createPauseHandler() {
        return pauseResume();
    }

    function createResumeHandler() {
        return pauseResume(true);
    }

    app.post('/queues/:queueName/pause', createPauseHandler());
    app.post('/queues/:queueName/resume', createResumeHandler());
    app.post('/queues/pause', createPauseHandler());
    app.post('/queues/resume', createResumeHandler());

    return Promise.resolve(libarka);
};
