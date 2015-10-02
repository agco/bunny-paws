'use strict';

var sinon = require('sinon');
var chai = require('chai');
var expect = chai.expect;
var Promise = require('bluebird');
chai.use(require('sinon-chai'));
var libarkaFactory = require('../lib/libarka.js');
var queueHelper = require('./queue.helper.js');

var config = require('./config.js');

var queueName = queueHelper.defaultQueueName;

describe('amqplib', function () {

    describe('ack', function () {
        var consumerChannel;
        var pool;

        beforeEach(function () {
            pool = queueHelper.createPool();
            return queueHelper.purge(pool).then(queueHelper.connectAsPublisher).then(function (channel) {
                return queueHelper.send(channel, queueName, 'abc', 'def');
            });
        });
        afterEach(function () {
            return queueHelper.cleanupPool(pool);
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
                return  queueHelper.connect(pool).then(function (channel) {
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
                        return  queueHelper.connect(pool).then(function (channel) {
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
            pool = queueHelper.createPool();
        });
        afterEach(function () {
            return queueHelper.cleanupPool(pool);
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
                return queueHelper.purge(pool).then(queueHelper.connectAsPublisher).then(function (channel) {
                    return queueHelper.send(channel, queueName, 'a', 'b', 'c', 'd', 'e');
                }).then(function () {
                        return queueHelper.connect(pool);
                    }).then(function (channel) {
                        consumerChannelA = channel;
                        return consumerChannelA.consume(queueName, consumerSpyA);
                    }).then(function () {
                        return queueHelper.connect(pool);
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
                return queueHelper.purge(pool).then(queueHelper.connectAsPublisher).then(function (channel) {
                    return queueHelper.send(channel, queueName, 'abc');
                }).then(function () {
                        return queueHelper.connect(pool).then(function (channel) {
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
            pool = queueHelper.createPool();
            return queueHelper.purge(pool).then(queueHelper.connectAsPublisher).then(function (channel) {
                return queueHelper.send(channel, queueName, 'a');
            }).then(function () {
                    return queueHelper.purge(pool, queueNameB).then(function (pool) {
                        return queueHelper.connectAsPublisher(pool, queueNameB);
                    });
                }).then(function (channel) {
                    queueHelper.send(channel, queueNameB, 'b');
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
            return queueHelper.cleanupPool(pool).finally(function () {
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
                    return queueHelper.connectAsPublisher(pool).then(function (channel) {
                        return queueHelper.send(channel, queueName, 'aa');
                    }).then(function () {
                            return queueHelper.connectAsPublisher(pool);
                        }).then(function (channel) {
                            return queueHelper.send(channel, queueNameB, 'bb');
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

        describe('and pause invoked for queueB', function () {
            beforeEach(function () {
                return libarka.pause(queueNameB);
            });

            describe('and another message is published', function () {
                beforeEach(function () {
                    return queueHelper.connectAsPublisher(pool).then(function (channel) {
                        return queueHelper.send(channel, queueName, 'aa');
                    }).then(function () {
                            return queueHelper.connectAsPublisher(pool);
                        }).then(function (channel) {
                            return queueHelper.send(channel, queueNameB, 'bb');
                        }).then(function () {
                            return Promise.delay(100);
                        });
                });
                it('should consume the message in queue A', function () {
                    expect(consumerSpyA).to.have.been.callCount(2);
                });
                it('should NOT consume the message in queue B', function () {
                    expect(consumerSpyB).to.have.been.callCount(1);
                });
                describe('and resume invoked for queueB', function () {
                    beforeEach(function () {
                        return libarka.resume(queueNameB);
                    });
                    it('should consume the message', function () {
                        expect(consumerSpyA).to.have.been.callCount(2);
                        expect(consumerSpyB).to.have.been.callCount(2);
                    });
                });
                describe('and resume invoked for all queues', function () {
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

        describe('and pause invoked for all queues', function () {
            beforeEach(function () {
                return libarka.pause();
            });

            describe('and another message is published', function () {
                beforeEach(function () {
                    return queueHelper.connectAsPublisher(pool).then(function (channel) {
                        return queueHelper.send(channel, queueName, 'aa');
                    }).then(function () {
                            return queueHelper.connectAsPublisher(pool);
                        }).then(function (channel) {
                            return queueHelper.send(channel, queueNameB, 'bb');
                        }).then(function () {
                            return Promise.delay(100);
                        });
                });
                it('should NOT consume the message', function () {
                    expect(consumerSpyA).to.have.been.callCount(1);
                    expect(consumerSpyB).to.have.been.callCount(1);
                });
                describe('and resume invoked for all queues', function () {
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

    describe('whem multiple libarka\'s operating on the same queue', function () {
        var pool;
        var libarkaA;
        var libarkaB;
        var consumerSpyA;
        var consumerSpyB;
        var publishChannel;
        beforeEach(function () {
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
            pool = queueHelper.createPool();
            return queueHelper.purge(pool).then(queueHelper.connectAsPublisher).then(function (channel) {
                publishChannel = channel;
            }).then(function () {
                    libarkaA = libarkaFactory();
                    libarkaB = libarkaFactory();
                    return libarkaA.connect(config.amqp.url).then(function (libarka) {
                        return libarka.consume(queueName, consumerSpyA);
                    });
                }).then(function () {
                    return libarkaB.connect(config.amqp.url).then(function (libarka) {
                        return libarka.consume(queueName, consumerSpyB);
                    });
                }).then(function () {
                    return queueHelper.send(publishChannel, queueName, 'a', 'b');
                }).then(function () {
                    return Promise.delay(100);
                });
        });
        afterEach(function () {
            return queueHelper.cleanupPool(pool).finally(function () {
                return libarkaA.disconnect().then(function () {
                    return libarkaB.disconnect();
                });
            });
        });
        describe('and 2 messages published', function () {
            it('should be consumed by each libarka\'s listeners', function () {
                expect(consumerSpyA).to.have.been.callCount(1);
                expect(consumerSpyB).to.have.been.callCount(1);
            });

            describe('and pause invoked', function () {
                beforeEach(function () {
                    return libarkaA.pause();
                });

                describe('and subsequent messages published', function () {
                    beforeEach(function () {
                        return queueHelper.send(publishChannel, queueName, 'c', 'd');
                    });
                    it('should NOT be consumed', function () {
                        expect(consumerSpyA).to.have.been.callCount(1);
                        expect(consumerSpyB).to.have.been.callCount(1);
                    });

                    describe('and resume invoked', function () {
                        beforeEach(function () {
                            return libarkaA.resume().then(function () {
                                return Promise.delay(100);
                            });
                        });
                        it('should consume the messages', function () {
                            expect(consumerSpyA.callCount + consumerSpyB.callCount).to.equal(4);
                        });
                    });

                });
            });
        });
    });
});
