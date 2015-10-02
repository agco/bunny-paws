'use strict';

var libarkaFactory = require('./libarka.js');

module.exports = function (app, amqpHttpApiBaseUrl, ampqVhost)
{
    var libarka = libarkaFactory();

    function getUrl(queueName)
    {
        return amqpHttpApiBaseUrl + '/api/queues/' + encodeURIComponent(ampqVhost) + '/' + (queueName || '');
    }

    function pause(req, res)
    {
        libarka.pause(req.params.queueName).then(function ()
        {
            var url = getUrl(req.params.queueName);
            res.set('Location', url).send({metrics: url}).status(204);
        }).catch(function (error)
                {
                    console.error(error && error.stack || error);
                    res.sendStatus(500);
                });
    }

    function resume(req, res)
    {
        libarka.resume(req.params.queueName).then(function ()
        {
            var url = getUrl(req.params.queueName);
            res.set('Location', url).send({metrics: url}).status(204);
        }).catch(function (error)
                {
                    console.error(error && error.stack || error);
                    res.sendStatus(500);
                });
    }

    app.post('/queues/:queueName/pause', pause);
    app.post('/queues/:queueName/resume', resume);
    app.post('/queues/pause', pause);
    app.post('/queues/resume', resume);

    return libarka.connect();
};
