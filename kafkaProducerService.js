const { Kafka, logLevel } = require('kafkajs');
const consts = require('./constants');

const kafka = new Kafka({
    logLevel: logLevel.INFO,
    brokers: [`${consts.KAFKA_HOST_PORT}`],
    clientId: 'retryProducer',
})

const producer = kafka.producer();

producer.connect();

const sendMessage = (message) => {
    console.log(`Sending message`);;
    message = [message]
    producer.send({
        topic: 'retry_topic',
        messages: message,
    })
};

module.exports = { sendMessage: sendMessage };
