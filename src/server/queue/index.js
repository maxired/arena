const _ = require('lodash');
const Bull = require('bull');
const Bee = require('bee-queue');
const redis = require('redis');

const path = require('path');

class Queues {
  constructor(config) {
    this._queues = {};

    this.useCdn = {
      value: true,
      get useCdn() {
        return this.value;
      },
      set useCdn(newValue) {
        this.value = newValue;
      }
    };

    this.setConfig(config);

    if(config.type === 'bee' && config.discovery){
      this.redis = redis.createClient(config);
    }
  }

  async list() {
    if (!this._config.discovery){
      return this._config.queues;
    }

    const self = this;
    return new Promise((resolve, reject) => {
      this.redis.keys(`bq:${this._config.prefix}*:id`, function(err, res) {
        self._config.queues = res.map(name => ({ name: name.slice(0, -3), hostId: self._config.hostId }));
        resolve(self._config.queues);
      })
    })
  }

  setConfig(config) {
    this._config = config;
  }

  async get(queueName, queueHost) {
    const queueConfig = _.find(this._config.queues, {
      name: queueName,
      hostId: queueHost
    });
    if (!queueConfig) return null;

    if (this._queues[queueHost] && this._queues[queueHost][queueName]) {
      return this._queues[queueHost][queueName];
    }

    const { type, name, port, host, db, password, prefix, url, redis, tls } = queueConfig;

    const redisHost = { host };
    if (password) redisHost.password = password;
    if (port) redisHost.port = port;
    if (db) redisHost.db = db;
    if (tls) redisHost.tls = tls;

    const isBee = type === 'bee';

    const options = {
      redis: redis || url || redisHost
    };
    if (prefix) options.prefix = prefix;

    let queue;
    if (isBee) {
      _.extend(options, {
        isWorker: false,
        getEvents: false,
        sendEvents: false,
        storeJobs: false
      });

      queue = new Bee(name, options);
      queue.IS_BEE = true;
    } else {
      queue = new Bull(name, options);
    }

    this._queues[queueHost] = this._queues[queueHost] || {};
    this._queues[queueHost][queueName] = queue;

    return queue;
  }
}

module.exports = Queues;
