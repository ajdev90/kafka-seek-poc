const { Kafka, logLevel } = require('kafkajs');
const consts = require('./constants');
const producerService = require('./kafkaProducerService');
const kafka = new Kafka({
    logLevel: logLevel.INFO,
    brokers: [`${consts.KAFKA_HOST_PORT}`],
    clientId: 'seek_consumer',
})

const topic = 'seek_topic'
const consumer = kafka.consumer({ groupId: 'seek_group' })

const subscribeConnect = async () => {
    console.log(`---subscribeConnect start---`);
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });
    console.log(`---subscribeConnect complete---`);
}

const run = async (seekOffset, partitionNum) => {
    console.log(`seekOffset=${seekOffset} and partition=${partitionNum}`);
    consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`topic=${topic}, partition=${partition}, offset=${message.offset}`);
            if (message.offset == seekOffset) {
                let msg = JSON.parse(message.value);
                console.log(`data=${msg}`);
                console.log(`typeof=${typeof (msg)}`);
                console.log(`message.value=${JSON.stringify(msg)}`);
                let messageToSend = {
                    key: null,
                    value: JSON.stringify(msg)
                }
                producerService.sendMessage(messageToSend);
                //Stop this consumer.
                consumer.stop();
                return true;
            }
        },
    });
    consumer.seek({ topic: topic, partition: partitionNum, offset: seekOffset });
}

subscribeConnect().catch((error)=>{
    console.error(error);
});

module.exports = {run:run};
