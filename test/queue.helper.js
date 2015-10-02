'use strict';

var _ = require('lodash');
var amqplib = require('amqplib');
var Promise = require('bluebird');
var config = require('./config.js');

var queueName = 'tasks';

function connect(pool) {
    return amqplib.connect(config.amqp.url).then(function (connection) {
        pool.connections = pool.connections || [];
        pool.connections.push(connection);
        return connection.createChannel();
    }).then(function (channel) {
            pool.channels = pool.channels || [];
            pool.channels.push(channel);
            return channel;
        });
}

function connectAsPublisher(pool, alternativeQueueName) {
    return connect(pool).then(function (channel) {
        channel.assertQueue(alternativeQueueName || queueName);
        return channel;
    });
}

function purge(pool, alternativeQueueName) {
    return connect(pool).then(function (channel) {
        var name = alternativeQueueName || queueName;
        channel.assertQueue(name);
        return channel.purgeQueue(name);
    },function (err) {
        console.error(err);
    }).then(function () {
            return pool;
        });
}

function send(channel, queueName) {
    var messages = _.toArray(arguments);
    messages.shift();
    messages.shift();
    return Promise.all(_.map(messages, function (item) {
        return channel.sendToQueue(queueName, new Buffer(item));
    }));
}

function createPool() {
    var pool = { channels: [], connections: []};
    pool.channelClosed = function (channel) {
        var indexOf = pool.channels.indexOf(channel);
        if (indexOf > -1) {
            pool.channels.splice(indexOf, 1);
        }
    };
    return pool;
}

function cleanupPool(pool) {
    return Promise.all(_.map(pool.channels, function (channel) {
            return channel && channel.close();
        })).then(function () {
            return Promise.all(_.map(pool.connections, function (connection) {
                return connection && connection.close();
            }));
        });
}
module.exports = {
    connect: connect,
    cleanupPool: cleanupPool,
    connectAsPublisher: connectAsPublisher,
    createPool: createPool,
    send: send,
    purge: purge,
    defaultQueueName: queueName
};
