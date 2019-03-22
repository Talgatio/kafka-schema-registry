# kafka-schema-registry

module for create kafka-node connection. 

## Installation and Usage

Before installing the package, the Kafka server must be running and Schema-registry
or you can running  [Confluent](https://www.confluent.io/ "Confluent")

Install the package

```
npm install kafka-schema-registry --save
```

Require KafkaClient:

```javascript
const KafkaClient = require('kafka-schema-registry');
const kafkaClient = new KafkaClient({
  schemaHost: 'http://your-kafka-host:8081',
  kafkaHost: 'your-kafka-host:9092',
  topics: [{topic: 'your-topic'}]
});
```

Producer

```javascript
const KafkaClient = require('kafka-schema-registry');

const kafkaClient = new KafkaClient({
  schemaHost: 'http://localhost:8081',
  kafkaHost: 'localhost:9092'
});

kafkaClient.sendMessage(message, topic, name, key);
```

Consumer

```javascript
const KafkaClient = require('kafka-schema-registry');
const kafkaClient = new KafkaClient({
  schemaHost: 'http://localhost:8081',
  kafkaHost: 'localhost:9092',
  topics: [{topic: 'your-topic'}]
});

kafkaClient.on('message', (data, err) => {
  if (err) console.log(err);
  else console.log(data);
});
```
