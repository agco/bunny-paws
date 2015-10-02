'use strict';

var express = require('express');
var libarkaModule = require('../lib/index.js');
var config = require('./config.js');
var queueHelper = require('./queue.helper.js');
var sinon = require('sinon');
var chai = require('chai');
chai.use(require('sinon-chai'));
var expect = chai.expect;
var Promise = require('bluebird');
var libarkaFactory = require('../lib/libarka.js');
var $http = require('http-as-promised');
var url = require('url');

var queueName = queueHelper.defaultQueueName;

describe('rest', function () {

    var app;
    var server;
    var libarka;
    var baseUrl;

    function makeUrl(href) {
        return url.resolve(baseUrl, href);
    }

    before(function () {
        app = express();
        server = app.listen(config.port);
        baseUrl = 'http://localhost:' + config.port;
        console.log('Listening on port', config.port);
        return libarkaModule.registerRoutes(app, config.amqp.httpApiBaseUrl, config.amqp.vhost).then(function (_libarka) {
            libarka = _libarka;
        });
    });

    after(function () {
        server.close();
        return libarka.disconnect();
    });

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
                return $http.post(makeUrl('/queues/pause'));
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
                        return $http.post(makeUrl('/queues/resume'));
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
                return $http.post(makeUrl('/queues/' + queueNameB + '/pause'));
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
                        return $http.post(makeUrl('/queues/' + queueNameB + '/resume'));
                    });
                    it('should consume the message', function () {
                        expect(consumerSpyA).to.have.been.callCount(2);
                        expect(consumerSpyB).to.have.been.callCount(2);
                    });
                });
                describe('and resume invoked for all queues', function () {
                    beforeEach(function () {
                        return $http.post(makeUrl('/queues/resume'));
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
                return $http.post(makeUrl('/queues/pause'));
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
                        return $http.post(makeUrl('/queues/resume'));
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
