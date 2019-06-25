const kafka = require('kafka-node');
const {Producer, ConsumerGroup} = kafka;

exports.kafkaClient = function (options) {
  const client = new kafka.KafkaClient({
    kafkaHost: options.kafkaHost || 'localhost:9092'
  });
  const producer = new Producer(client);

  const opts = {
    kafkaHost: options.kafkaHost,
    groupId: 'node-consumer-group',
    encoding: 'buffer',
    fromOffset: 'latest'
  };

  const consumer = topics => new ConsumerGroup(opts, topics);

  return {
    consumer,
    producer
  }
};
