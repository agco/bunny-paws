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
                        item.pause();
                    } catch (e) {
                        console.warn('Cannot cancel ', item.consumerTag, ' on channel', e && e.stack || e);
                    }
                    item.paused = true;
                }
            });
        } else if (data && 'resume' === data.toString()) {
            console.info(data.toString(), msg.fields.routingKey);
            _.forEach(that.consumers, function (item, channelName) {
                if ((msg.fields.routingKey === '#' || msg.fields.routingKey === channelName) && item.paused) {
                    try {
                        item.resume();
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
    this.consumers = this.consumers || {};
    this.consumers[queue.name] = queue;
    return this;
};

module.exports = Libarka;
