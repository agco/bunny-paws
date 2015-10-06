'use strict';

var _ = require('lodash');
var jackrabbit = require('jackrabbit');
var Promise = require('bluebird');

function Libarka(connectionUrl, config) {
    var defaultConfig = {
        systemChannelName: 'consumers.ctrl.topic'
    };
    this.config = _.extend(defaultConfig, config);
    var that = this;
    this.rabbit = jackrabbit(connectionUrl);
    this.systemTopic = this.rabbit.topic(this.config.systemChannelName);
    this.systemTopic.queue({exclusive: true, key: '#'}).consume(function (data, ack, nack, msg) {
        ack();
        if (data && 'pause' === data.toString()) {
            console.info(data.toString(), msg.fields.routingKey);
            _.forEach(that.consumers, function (item, channelName) {
                if (msg.fields.routingKey === '#' || msg.fields.routingKey === channelName) {
                    try {
                        item.channel.cancel(item.consumerTag);
                    } catch (e) {
                        console.warn('Cannot cancel ', item.consumerTag, ' on channel', e && e.stack || e);
                    }
                    item.cancelled = true;
                }
            });
        } else if (data && 'resume' === data.toString()) {
            console.info(data.toString(), msg.fields.routingKey);
            _.forEach(that.consumers, function (item, channelName) {
                if ((msg.fields.routingKey === '#' || msg.fields.routingKey === channelName) && item.cancelled) {
                    try {
                        item.queue.consume(item.callback);
                    } catch (e) {
                        console.warn('Cannot consume(resume) channel', channelName, e && e.stack || e);
                    }
                }
            });
        }
    });
}

Libarka.prototype.pause = function (queue) {
    var that = this;
    this.systemTopic.publish('pause', {key: queue || '#'});
    return new Promise(function (resolve, reject) {
        that.systemTopic.once('drain', function (err) {
            if (err) {
                reject(err);
            }
            else {
                resolve(that);
            }
        });
    });
};

Libarka.prototype.resume = function (queue) {
    var that = this;
    this.systemTopic.publish('resume', {key: queue || '#'});
    return new Promise(function (resolve, reject) {
        that.systemTopic.once('drain', function (err) {
            if (err) {
                reject(err);
            }
            else {
                resolve(that);
            }
        });
    });
};

Libarka.prototype.disconnect = function () {
    var that = this;
    return new Promise(function (resolve, reject) {
        if (that.rabbit) {
            that.rabbit.close(function (err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(that);
                }
            });
        } else {
            resolve(that);
        }
        that.rabbit = null;
        that.systemTopic = null;
    });
};

Libarka.prototype.addPauseResume = function (queue) {
    var that = this;
    var consumer = {
        queue: queue
    };
    queue.once('connected', function (channel) {
        consumer.channel = channel;
    });

    return {
        consume: function (callback, options) {
            if (null != consumer.callback) {
                throw new Error('Consumer already registered');
            }
            that.consumers = that.consumers || {};
            consumer.callback = callback;
            that.consumers[queue.name] = consumer;
            queue.once('consuming', function (_tag) {
                consumer.consumerTag = _tag;
            });
            queue.consume(callback, options);
        }
    };
};

module.exports = Libarka;
