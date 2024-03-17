"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaService = void 0;
const kafkajs_1 = require("kafkajs");
class KafkaService {
    constructor() {
        const brokers = process.env.KAFKA_BROKERS?.split(',') ?? ['localhost:9092'];
        const clientId = process.env.KAFKA_CLIENT_ID ?? 'my-app';
        const requestTimeout = process.env?.KAFKA_REQUEST_TIMEOUT ?? 30000;
        const retry = process.env?.KAFKA_RETRY ?? 8;
        const initialRetryTime = process.env?.KAFKA_INITIAL_RETRY_TIME ?? 100;
        const logLevel = process.env?.KAFKA_LOG_LEVEL ?? 0;
        this.kafka = new kafkajs_1.Kafka({
            clientId,
            brokers,
            requestTimeout: Number(requestTimeout),
            retry: {
                initialRetryTime: Number(initialRetryTime),
                retries: Number(retry),
            },
        });
        this.logger = this.kafka.logger();
        this.admin = this.kafka.admin();
        this.logger.info('KafkaService initialized', {
            brokers,
            clientId,
            requestTimeout,
            retry,
            initialRetryTime,
            logLevel,
        });
    }
    async createTopic(topic) {
        const topics = topic.split(',').map((topic) => ({ topic }));
        await this.admin.connect();
        await this.admin.createTopics({
            topics: topics,
        });
        this.logger.info(`Created topic: ${topic}`);
        await this.admin.disconnect();
    }
    async sendMessage(topic, message, headers) {
        if (!headers) {
            headers = {};
        }
        let messages = [];
        if (typeof message === 'object') {
            if (Array.isArray(message)) {
                message.forEach((msg) => {
                    messages.push({ headers, value: JSON.stringify(msg) });
                });
            }
            else {
                messages.push({ headers, value: JSON.stringify(message) });
            }
        }
        else {
            messages.push({ headers, value: message });
        }
        const producer = this.kafka.producer();
        await producer.connect();
        const record = await producer.send({ topic, messages });
        this.logger.info(`Sent successfully topic:${topic}`, { ...record });
        await producer.disconnect();
        return record;
    }
    async consumeMessages(topic, callback) {
        const consumer = this.kafka.consumer({ groupId: 'test-group' });
        await consumer.connect();
        await consumer.subscribe({ topics: topic.split(','), fromBeginning: true });
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const { headers, offset, value } = message;
                const payload = value?.toString() ?? '';
                for (const key in headers) {
                    if (headers?.hasOwnProperty(key) && Buffer.isBuffer(headers[key])) {
                        headers[key] = headers[key]?.toString();
                    }
                }
                this.logger.info('received message', {
                    topic,
                    partition,
                    offset,
                    value: payload,
                    headers: headers,
                });
                callback(topic, payload);
            },
        });
    }
}
exports.KafkaService = KafkaService;
