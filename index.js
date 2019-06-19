const kafkaClient = require('./lib/kafkaClient');
const avsc = require('avsc');
const EventEmitter = require('events');

const url = require('url');
const http = require('http');
const https = require('https');

const SchemaCache = require('avro-schema-registry/lib/schema-cache');

class KafkaClient extends EventEmitter {
  constructor(options) {
    super();
    this.kafka = kafkaClient.kafkaClient(options);
    this.registry = require('avro-schema-registry')(options.schemaHost || 'http://localhost:8081');
    this.consumer = null;
    this.producer = this.kafka.producer;

    const parsed = url.parse(options.schemaHost || 'http://localhost:8081');
    this.registryOptions = {
      cache: new SchemaCache(),
      protocol: parsed.protocol.startsWith('https') ? https : http,
      host: parsed.hostname,
      port: parsed.port,
      path: parsed.path,
    };

    if (options.topics) {
      this.consumer = this.kafka.consumer(options.topics);
      this.initListen();
    }
  }

  initListen() {
    this.consumer.on('message', (msg) => {
      this.registry.decode(msg.value).then(val => {
        val.topicKey = msg.key;
        this.emit(msg.topic, val);
      })
    });
  }

  sendMessage(message, topic, key) {
    this.byLatest(topic, message)
      .then((msg) => {
        const sendMsg = [{ topic, key, messages: msg }];
        this.producer.send(sendMsg, (err, data) => {
          if (err) console.error('Trouble with publishing to kafka, %s', err.message || err);
        })
      })
      .catch((err) => {
        console.error(`Can\'t get latest schema version`, err)
      });
  }

  byLatest(topic, msg, parseOptions = null) {
    return new Promise((resolve, reject) => {
      let latest = this.getLatest(this.registryOptions, topic, parseOptions);
      latest.then(({schemaId}) => {
        let promise = this.registryOptions.cache.getById(schemaId);

        if (promise) {
          return resolve(promise);
        }

        promise = latest;
        this.registryOptions.cache.set(schemaId, promise);

        promise
          .then((result) => this.registryOptions.cache.set(schemaId, result))
          .catch(reject);

        return resolve(promise);
      })
    })
      .then(({schemaId, schema}) => {
        const encodedMessage = schema.toBuffer(msg);

        const message = Buffer.alloc(encodedMessage.length + 5);
        message.writeUInt8(0);
        message.writeUInt32BE(schemaId, 1);
        encodedMessage.copy(message, 5);
        return message;
      });
  }

  getLatest(registry, topic, parseOptions = {}) {
    return new Promise((resolve, reject) => {
      const {protocol, host, port, auth, path} = registry;
      const requestOptions = {
        host,
        port,
        path: `${path}subjects/${topic}-value/versions/latest`,
        auth
      };
      protocol.get(requestOptions, (res) => {
        let data = '';
        res.on('data', (d) => {
          data += d;
        });
        res.on('error', (e) => {
          reject(e);
        });
        res.on('end', () => {
          if (res.statusCode !== 200) {
            const error = JSON.parse(data);
            return reject(new Error(`Schema registry error: ${error.error_code} - ${error.message}`));
          }

          const schema = JSON.parse(data).schema;
          const schemaId = JSON.parse(data).id;
          resolve({
            schema: avsc.parse(schema, parseOptions),
            schemaId
          });
        });
      }).on('error', (e) => {
        reject(e);
      });
    });
  }

}

module.exports = KafkaClient;
