'use strict';

var _ = require('lodash');
var amqplib = require('amqplib');
var sinon = require('sinon');
var chai = require('chai');
var expect = chai.expect;
var Promise = require('bluebird');
chai.use(require('sinon-chai'));
var libarkaFactory = require('../lib/libarka.js');

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

describe('amqplib', function () {

    describe('ack', function () {
        var consumerChannel;
        var pool;

        beforeEach(function () {
            pool = createPool();
            return purge(pool).then(connectAsPublisher).then(function (channel) {
                return send(channel, queueName, 'abc', 'def');
            });
        });
        afterEach(function () {
            return cleanupPool(pool);
        });
        describe('when first message is NOT acknowledged', function () {
            var consumerSpy;
            beforeEach(function () {
                var callCount = 0;
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
                it('should receive all messages', function () {
                    expect(consumerSpy).to.have.been.callCount(3);
                });
            });
            describe('and consumer closes and another consumer connects', function () {
                beforeEach(function () {
                    return consumerChannel.close().then(function () {
                        pool.channelClosed(consumerChannel);
                        return  connect(pool).then(function (channel) {
                            consumerChannel = channel;
                            return channel.consume(queueName, consumerSpy);
                        });
                    });
                });
                it('should receive all the messages again', function () {
                    expect(consumerSpy).to.have.been.callCount(3);
                });
            });
        });
    });
    describe('nack', function () {

        var pool;
        beforeEach(function () {
            pool = createPool();
        });
        afterEach(function () {
            return cleanupPool(pool);
        });
        describe('when there are 2 consumers, one always rejecting and another always accepting', function () {
            var consumerSpyA;
            var consumerSpyB;
            beforeEach(function () {
                var consumerChannelA;
                var consumerChannelB;
                consumerSpyA = sinon.spy(function (msg) {
                    consumerChannelA.nack(msg);
                });
                consumerSpyB = sinon.spy(function (msg) {
                    consumerChannelB.ack(msg);
                });
                return purge(pool).then(connectAsPublisher).then(function (channel) {
                    return send(channel, queueName, 'a', 'b', 'c', 'd', 'e');
                }).then(function () {
                        return  connect(pool);
                    }).then(function (channel) {
                        consumerChannelA = channel;
                        return consumerChannelA.consume(queueName, consumerSpyA);
                    }).then(function () {
                        return  connect(pool);
                    }).then(function (channel) {
                        consumerChannelB = channel;
                        return consumerChannelB.consume(queueName, consumerSpyB);
                    }).then(function () {
                        return Promise.delay(500);
                    });
            });

            it('should finally get all messages consumed', function () {
                expect(consumerSpyA.callCount).to.be.above(0);
                expect(consumerSpyB).to.have.been.callCount(5);
            });
        });
        describe('when message is rejected for the first time', function () {
            var consumerSpy;
            beforeEach(function () {
                var consumerChannel;
                var callCount = 0;
                consumerSpy = sinon.spy(function (msg) {
                    if (callCount) {
                        consumerChannel.ack(msg);
                    } else {
                        consumerChannel.nack(msg);
                    }
                    callCount++;
                });
                return purge(pool).then(connectAsPublisher).then(function (channel) {
                    return send(channel, queueName, 'abc');
                }).then(function () {
                        return  connect(pool).then(function (channel) {
                            consumerChannel = channel;
                            return consumerChannel.consume(queueName, consumerSpy);
                        });
                    }).then(function () {
                        return Promise.delay(100);
                    });
            });
            it('should be redelivered to the same (only one) consumer', function () {
                expect(consumerSpy).to.have.been.callCount(2);
            });
        });
    });
});


describe('libarka', function () {
    describe('when message is published', function () {
        var pool;
        var consumerSpyA;
        var consumerSpyB;
        var libarka;
        var queueNameB = 'szazalakacu';

        beforeEach(function () {
            pool = createPool();
            return purge(pool).then(connectAsPublisher).then(function (channel) {
                return send(channel, queueName, 'a');
            }).then(function () {
                    return purge(pool, queueNameB).then(function (pool) {
                        return connectAsPublisher(pool, queueNameB);
                    });
                }).then(function (channel) {
                    send(channel, queueNameB, 'b');
                }).then(function () {
                    consumerSpyA = sinon.spy(function (msg) {
                        if (msg) {
                            this.ack(msg);
                        }
                    });
                    consumerSpyB = sinon.spy(function (msg) {
                        if (msg) {
                            this.ack(msg);
                        }
                    });
                    libarka = libarkaFactory();
                    return libarka.connect(config.amqp.url).then(function (libarka) {
                        return libarka.consume(queueName, consumerSpyA);
                    }).then(function () {
                            return libarka.consume(queueNameB, consumerSpyB);
                        });
                }).then(function () {
                    return Promise.delay(100);
                });
        });
        afterEach(function () {
            return cleanupPool(pool).finally(function () {
                return libarka.disconnect();
            });
        });
        it('should be consumed', function () {
            expect(consumerSpyA).to.have.been.callCount(1);
            expect(consumerSpyB).to.have.been.callCount(1);
        });

        describe('and pause invoked', function () {
            beforeEach(function () {
                return libarka.pause();
            });
            describe('and another message is published', function () {
                beforeEach(function () {
                    return connectAsPublisher(pool).then(function (channel) {
                        return send(channel, queueName, 'aa');
                    }).then(function () {
                            return connectAsPublisher(pool);
                        }).then(function (channel) {
                            return send(channel, queueNameB, 'bb');
                        }).then(function () {
                            return Promise.delay(100);
                        });
                });
                it('should not consume the message in queue', function () {
                    expect(consumerSpyA).to.have.been.callCount(1);
                    expect(consumerSpyB).to.have.been.callCount(1);
                });
                describe('and resume invoked', function () {
                    beforeEach(function () {
                        return libarka.resume();
                    });
                    it('should consume the message', function () {
                        expect(consumerSpyA).to.have.been.callCount(2);
                        expect(consumerSpyB).to.have.been.callCount(2);
                    });
                });

            });
        });
    });
});
