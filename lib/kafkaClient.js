const kafka = require('kafka-node');
const {Consumer, Producer} = kafka;

exports.kafkaClient = function (options) {
  const client = new kafka.KafkaClient({
    kafkaHost: options.kafkaHost || 'localhost:9092'
  });
  const producer = new Producer(client);

  const consumer = topics => new Consumer(
    client,
    topics,
    {
      encoding: 'buffer',
    },
  );

  return {
    consumer,
    producer
  }
};




