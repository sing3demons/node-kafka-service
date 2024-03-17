"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaService = void 0;
const kafkajs_1 = require("kafkajs");
class KafkaService {
    constructor() {
        var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l;
        const brokers = (_b = (_a = process.env.KAFKA_BROKERS) === null || _a === void 0 ? void 0 : _a.split(',')) !== null && _b !== void 0 ? _b : ['localhost:9092'];
        const clientId = (_c = process.env.KAFKA_CLIENT_ID) !== null && _c !== void 0 ? _c : 'my-app';
        const requestTimeout = (_e = (_d = process.env) === null || _d === void 0 ? void 0 : _d.KAFKA_REQUEST_TIMEOUT) !== null && _e !== void 0 ? _e : 30000;
        const retry = (_g = (_f = process.env) === null || _f === void 0 ? void 0 : _f.KAFKA_RETRY) !== null && _g !== void 0 ? _g : 8;
        const initialRetryTime = (_j = (_h = process.env) === null || _h === void 0 ? void 0 : _h.KAFKA_INITIAL_RETRY_TIME) !== null && _j !== void 0 ? _j : 100;
        const logLevel = (_l = (_k = process.env) === null || _k === void 0 ? void 0 : _k.KAFKA_LOG_LEVEL) !== null && _l !== void 0 ? _l : 0;
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
    }
    createTopic(topic) {
        return __awaiter(this, void 0, void 0, function* () {
            const topics = topic.split(',').map((topic) => ({ topic }));
            yield this.admin.connect();
            yield this.admin.createTopics({
                topics: topics,
            });
            this.logger.info(`Created topic: ${topic}`);
            yield this.admin.disconnect();
        });
    }
    sendMessage(topic, message, headers) {
        return __awaiter(this, void 0, void 0, function* () {
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
            yield producer.connect();
            const record = yield producer.send({ topic, messages });
            this.logger.info(`Sent successfully topic:${topic}`, Object.assign({}, record));
            yield producer.disconnect();
            return record;
        });
    }
    consumeMessages(topic, callback) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumer = this.kafka.consumer({ groupId: 'test-group' });
            yield consumer.connect();
            yield consumer.subscribe({ topics: topic.split(','), fromBeginning: true });
            yield consumer.run({
                eachMessage: (_a) => __awaiter(this, [_a], void 0, function* ({ topic, partition, message }) {
                    var _b, _c;
                    const { headers, offset, value } = message;
                    const payload = (_b = value === null || value === void 0 ? void 0 : value.toString()) !== null && _b !== void 0 ? _b : '';
                    for (const key in headers) {
                        if ((headers === null || headers === void 0 ? void 0 : headers.hasOwnProperty(key)) && Buffer.isBuffer(headers[key])) {
                            headers[key] = (_c = headers[key]) === null || _c === void 0 ? void 0 : _c.toString();
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
                }),
            });
        });
    }
}
exports.KafkaService = KafkaService;
