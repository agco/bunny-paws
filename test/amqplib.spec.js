var _ = require('lodash');
var amqplib = require('amqplib');
var sinon = require('sinon');
var chai = require('chai');
var expect = chai.expect;
var Promise = require('bluebird');
chai.use(require('sinon-chai'));

var config = require('./config.js');

var queueName = 'tasks';

describe('amqplib', function () {

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

    function connectAsPublisher(pool) {
        return connect(pool).then(function (channel) {
            channel.assertQueue(queueName);
            return channel;
        });
    }

    function purge(pool) {
        return connect(pool).then(function (channel) {
            return channel.purgeQueue(queueName);
        },function (err) {
            console.log(err);
        }).then(function () {
                return pool;
            });
    }

    function send(channel) {
        var messages = _.toArray(arguments);
        messages.shift();
        return Promise.all(_.map(messages, function (item) {
            return channel.sendToQueue(queueName, new Buffer(item));
        }));
    }

    describe('ack', function () {
        var consumerChannel;
        var pool = {};

        beforeEach(function () {
            pool = {};
            return purge(pool).then(connectAsPublisher).then(function (channel) {
                return send(channel, 'abc', 'def');
            });
        });
        afterEach(function () {
            return Promise.all(_.map(pool.channels, function (channel) {
                    return channel && channel.close();
                })).then(function () {
                    return Promise.all(_.map(pool.connections, function (connection) {
                        return connection && connection.close();
                    }));
                });
        });
        describe('when first message is NOT acknowledged', function () {
            var consumerSpy;
            var callCount;
            beforeEach(function () {
                callCount = 0;
                consumerSpy = sinon.spy(function (msg) {
                    if (callCount) {
                        consumerChannel.ack(msg);
                    }
                    callCount++;
                });
                return  connect(pool).then(function (channel) {
                    consumerChannel = channel;
                    return consumerChannel.consume(queueName, consumerSpy);
                });
            });
            describe('and consumer recovers', function () {
                beforeEach(function () {
                    return consumerChannel.recover();
                });
                it.only('should receive all messages', function () {
                    expect(consumerSpy).to.have.been.callCount(3);
                });
            });
            describe('and consumer closes and another consumer connects', function () {
                it('should receive all the messages again', function () {
                    throw new Error('Not implemented yet');
                });
            });
        });
    });
    describe('nack', function () {
        describe('when there are 2 consumers, one always rejecting and another always accepting', function () {
            it('should finally get all messages consumed', function () {
                throw new Error('Not implemented yet');
            });
        });
        describe('when message is rejected for the first time', function () {
            it('should be redelivered to the same (only one) consumer', function () {
                throw new Error('Not implemented yet');
            });
        });
    });
});


describe('libarka', function () {
    describe('when message is published', function () {
        it('should be consumed', function () {
            throw new Error('Not implemented yet');
        });

        describe('and pause message is issued on system channel', function () {
            describe('and another message is published', function () {
                it('should not consume the message in queue', function () {
                    throw new Error('Not implemented yet');
                });
                describe('and resume message is issued on system channel', function () {
                    it('should consume the message', function () {
                        throw new Error('Not implemented yet');
                    });
                });

            });
        });
    });
});
