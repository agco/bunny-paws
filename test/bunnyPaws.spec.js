'use strict';

var sinon = require('sinon');
var chai = require('chai');
var expect = chai.expect;
var Promise = require('bluebird');
chai.use(require('sinon-chai'));
var jackrabbit = require('jackrabbit');
var BunnyPaws = require('../lib/bunnyPaws.js');

var config = require('./config.js');


describe('BunnyPaws', function () {
    describe('when message is published', function () {
        var consumerSpyA;
        var consumerSpyB;
        var bunnyPaws;
        var queueNameA = 'tasks';
        var queueNameB = 'szazalakacu';
        var publishingRabbit;
        var consumingRabbit;

        function purgeQueues() {
            var purgatoryRabbit = jackrabbit(config.amqp.url);
            return new Promise(function (resolve, reject) {
                var defaultExchange = purgatoryRabbit.default();
                defaultExchange.on('connected', function (channel) {
                    var promiseA = new Promise(function (resolve, reject) {
                        channel.purgeQueue(queueNameA, function (err) {
                            if (err) {
                                reject(err);
                            }
                            else {
                                resolve();
                            }
                        });
                    });
                    var promiseB = new Promise(function (resolve, reject) {
                        channel.purgeQueue(queueNameB, function (err) {
                            if (err) {
                                reject(err);
                            }
                            else {
                                resolve();
                            }
                        });
                    });
                    Promise.all([promiseA, promiseB]).then(resolve, reject);
                });
            }).finally(function () {
                    return Promise.delay(100).then(function () {
                        purgatoryRabbit.close();
                    });
                });
        }

        beforeEach(function () {
            return purgeQueues().then(function () {
                publishingRabbit = jackrabbit(config.amqp.url);
                publishingRabbit.default()
                    .publish('a', { key: queueNameA })
                    .publish('b', { key: queueNameB });

                consumerSpyA = sinon.spy(function (data, ack) {
                    ack();
                });
                consumerSpyB = sinon.spy(function (data, ack) {
                    ack();
                });
                bunnyPaws = new BunnyPaws(config.amqp.url);
                consumingRabbit = jackrabbit(config.amqp.url);
                var queueA = consumingRabbit.default().queue({ name: queueNameA });
                var queueB = consumingRabbit.default().queue({ name: queueNameB });
                bunnyPaws.addPauseResume(queueA.consume(consumerSpyA));
                bunnyPaws.addPauseResume(queueB.consume(consumerSpyB));

                return Promise.delay(500);
            });
        });
        afterEach(function () {
            return new Promise(function (resolve, reject) {
                if (consumingRabbit) {
                    consumingRabbit.close(function (err) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve();
                        }
                    });
                } else {
                    resolve();
                }
            }).then(function () {
                    return new Promise(function (resolve, reject) {
                        if (publishingRabbit) {
                            publishingRabbit.close(function (err) {
                                if (err) {
                                    reject(err);
                                }
                                else {
                                    resolve();
                                }
                            });
                        }
                    });
                }).then(function () {
                    return bunnyPaws.disconnect();
                });
        });

        it('should be consumed', function () {
            expect(consumerSpyA).to.have.been.callCount(1);
            expect(consumerSpyB).to.have.been.callCount(1);
        });

        describe('and pause invoked', function () {
            beforeEach(function () {
                return bunnyPaws.pause().then(function () {
                    return Promise.delay(500);
                });
            });
            describe('and another message is published', function () {
                beforeEach(function () {
                    publishingRabbit.default()
                        .publish('aa', { key: queueNameA })
                        .publish('bb', { key: queueNameB });

                    return Promise.delay(100);
                });
                it('should not consume the message in queue', function () {
                    expect(consumerSpyA).to.have.been.callCount(1);
                    expect(consumerSpyB).to.have.been.callCount(1);
                });
                describe('and resume invoked', function () {
                    beforeEach(function () {
                        return bunnyPaws.resume().then(function () {
                            return Promise.delay(100);
                        });
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
                return bunnyPaws.pause(queueNameB);
            });

            describe('and another message is published', function () {
                beforeEach(function () {
                    publishingRabbit.default()
                        .publish('aa', { key: queueNameA })
                        .publish('bb', { key: queueNameB });

                    return Promise.delay(100);
                });
                it('should consume the message in queue A', function () {
                    expect(consumerSpyA).to.have.been.callCount(2);
                });
                it('should NOT consume the message in queue B', function () {
                    expect(consumerSpyB).to.have.been.callCount(1);
                });
                describe('and resume invoked for queueB', function () {
                    beforeEach(function () {
                        return bunnyPaws.resume(queueNameB).then(function () {
                            return Promise.delay(100);
                        });
                    });
                    it('should consume the message', function () {
                        expect(consumerSpyA).to.have.been.callCount(2);
                        expect(consumerSpyB).to.have.been.callCount(2);
                    });
                });
                describe('and resume invoked for all queues', function () {
                    beforeEach(function () {
                        return bunnyPaws.resume().then(function () {
                            return Promise.delay(100);
                        });
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
                return bunnyPaws.pause().then(function () {
                    return Promise.delay(100);
                });
            });

            describe('and another message is published', function () {
                beforeEach(function () {
                    publishingRabbit.default()
                        .publish('aa', { key: queueNameA })
                        .publish('bb', { key: queueNameB });
                    return Promise.delay(100);
                });
                it('should NOT consume the message', function () {
                    expect(consumerSpyA).to.have.been.callCount(1);
                    expect(consumerSpyB).to.have.been.callCount(1);
                });
                describe('and resume invoked for all queues', function () {
                    beforeEach(function () {
                        return bunnyPaws.resume().then(function () {
                            return Promise.delay(100);
                        });
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
