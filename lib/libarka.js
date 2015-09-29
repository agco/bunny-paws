'use strict';

var _ = require('lodash');
var amqplib = require('amqplib');
var Promise = require('bluebird');

function Libarka(config) {
    var defaultConfig = {
        systemChannelName: 'consumers.ctrl.topic'
    };
    this.config = _.extend(defaultConfig, config);
}

Libarka.prototype.connect = function (connectionUrl) {
    var that = this;
    return amqplib.connect(connectionUrl).then(function (connection) {
        that.connection = connection;
        return that._assertSystemExchange().then(function () {
            return that.systemChannel.assertQueue('', {exclusive: true}).then(function (queue) {
                that.systemChannel.bindQueue(queue.queue, that.config.systemChannelName, '#');
                return queue;
            }).then(function (queue) {
                    return that.systemChannel.consume(queue.queue, function (msg) {
                        if (!msg) {
                            return;
                        }
                        that.systemChannel.ack(msg);
                        if (msg.content && 'pause' === msg.content.toString()) {
                            console.info(msg.content.toString());
                            if (that.channel) {
                                _.forEach(that.consumers, function (item, channelName) {
                                    if (msg.fields.routingKey === '#' || msg.fields.routingKey === channelName) {
                                        that.channel.cancel(item.consumerTag);
                                        item.cancelled = true;
                                    }
                                });
                            }
                        } else if (msg.content && 'resume' === msg.content.toString()) {
                            console.info(msg.content.toString());
                            if (that.channel) {
                                _.forEach(that.consumers, function (item, channelName) {
                                    if ((msg.fields.routingKey === '#' || msg.fields.routingKey === channelName) && item.cancelled) {
                                        that.consume(channelName, item.callback);
                                    }
                                });
                            }
                        }
                    });
                });
        }).then(function () {
                return that;
            });
    });
};

Libarka.prototype.consume = function (channelName, callback) {
    if (!this.connection) {
        return Promise.reject(new Error('No connection, connect before consuming'));
    }
    var that = this;
    this.consumers = this.consumers || {};
//    TODO what do we do with overwriting? should we unregister such consumer?
    if (null != this.consumers[channelName] && !this.consumers[channelName].cancelled) {
        throw new Error('Consumer for "' + channelName + '" already registered');
    }
    this.consumers[channelName] = {callback: callback};
    return this._assertChannel().then(function () {
        return that.channel.consume(channelName,function (msg) {
            callback.call(that.channel, msg);
        }).then(function (result) {
                that.consumers[channelName].consumerTag = result.consumerTag;
                return result;
            });
    });
};

Libarka.prototype._assertChannel = function () {
    if (this.channel) {
        return Promise.resolve(this);
    } else {
        var that = this;
        if (!this.connection) {
            return Promise.reject(new Error('No connection, connect before creating channel'));
        }
        return this.connection.createChannel().then(function (channel) {
            that.channel = channel;
            return that;
        });
    }
};

Libarka.prototype._assertSystemChannel = function () {
    if (this.systemChannel) {
        return Promise.resolve(this);
    } else {
        var that = this;
        if (!this.connection) {
            return Promise.reject(new Error('No connection, connect before creating channel'));
        }
        return this.connection.createChannel().then(function (channel) {
            that.systemChannel = channel;
            return that;
        });
    }
};

Libarka.prototype._assertSystemExchange = function () {
    var that = this;
    return this._assertSystemChannel().then(function () {
        return that.systemChannel.assertExchange(that.config.systemChannelName, 'topic');
    });
};

Libarka.prototype.pause = function (queue) {
    var that = this;
    return this._assertSystemExchange().then(function () {
        queue = queue || '#';
        return that.systemChannel.publish(that.config.systemChannelName, queue, new Buffer('pause'));
    }).then(function () {
            // TODO this is really temporary
            return Promise.delay(100);
        });
};

Libarka.prototype.resume = function (queue) {
    var that = this;
    return this._assertSystemExchange().then(function () {
        queue = queue || '#';
        return that.systemChannel.publish(that.config.systemChannelName, queue, new Buffer('resume'));
    }).then(function () {
            // TODO this is really temporary
            return Promise.delay(100);
        });
};

Libarka.prototype.disconnect = function () {
    var that = this;
    return Promise.resolve(function () {
        if (that.channel) {
            return that.channel.close().finally(function () {
                delete that.channel;
            });
        } else {
            return null;
        }
    }).then(function () {
            if (that.systemChannel) {
                return that.systemChannel.close().finally(function () {
                    delete that.systemChannel;
                });
            } else {
                return null;
            }
        }).then(function () {
            if (that.connection) {
                return that.connection.close().finally(function () {
                    delete that.connection;
                });
            } else {
                return null;
            }
        });
};

module.exports = function () {
    return new Libarka();
};


