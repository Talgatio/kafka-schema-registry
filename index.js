const kafkaClient = require('./lib/kafkaClient');
const avro = require('avsc');
const EventEmitter = require('events');

class KafkaClient extends EventEmitter {
  constructor(options) {
    super();
    this.kafka = kafkaClient.kafkaClient(options);
    this.registry = require('avro-schema-registry')(options.schemaHost || 'http://localhost:8081');
    this.consumer = null;
    this.producer = this.kafka.producer;

    if (options.topics) {
      this.consumer = this.kafka.consumer(options.topics);
      this.initListen();
    }
  }

  initListen() {
    this.consumer.on('message', (msg) => {
      this.registry.decode(msg.value).then(val => {
        this.emit('message', val);
      })
    });
  }

  sendMessage(message, topic, name, key) {
    const avroSchema = avro.Type.forValue(message);
    const avroSchemaFields = avroSchema.schema({exportAttrs: false});
    const avroFull = {
      name,
      ...avroSchemaFields
    };
    this.registry.encodeMessage(topic, avroFull, message)
      .then((msg) => {
        const sendMsg = [{
          topic,
          key,
          messages: msg
        }];
        this.producer.send(sendMsg, (err, data) => {
          if (err) {
            console.log('Trouble with publishing to kafka, %s', err.message || err);
          }
        })
      })
  }

}

module.exports = KafkaClient;
