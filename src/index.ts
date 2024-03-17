import { Kafka, Message, Logger, Admin, IHeaders, ITopicConfig } from 'kafkajs'

type MessageCallback = (topic: string, message: string | undefined) => void

export class KafkaService {
  private kafka: Kafka
  private logger: Logger
  private admin: Admin

  constructor() {
    const brokers = process.env.KAFKA_BROKERS?.split(',') ?? ['localhost:9092']
    const clientId = process.env.KAFKA_CLIENT_ID ?? 'my-app'
    const requestTimeout = process.env?.KAFKA_REQUEST_TIMEOUT ?? 30000
    const retry = process.env?.KAFKA_RETRY ?? 8
    const initialRetryTime = process.env?.KAFKA_INITIAL_RETRY_TIME ?? 100
    const logLevel = process.env?.KAFKA_LOG_LEVEL ?? 0
    this.kafka = new Kafka({
      clientId,
      brokers,
      requestTimeout: Number(requestTimeout),
      retry: {
        initialRetryTime: Number(initialRetryTime),
        retries: Number(retry),
      },
    })
    this.logger = this.kafka.logger()
    this.admin = this.kafka.admin()

    this.logger.info('KafkaService initialized', {
      brokers,
      clientId,
      requestTimeout,
      retry,
      initialRetryTime,
      logLevel,
    })
  }

  async createTopic(topic: string) {
    const topics: ITopicConfig[] = topic.split(',').map((topic) => ({ topic }))

    await this.admin.connect()
    await this.admin.createTopics({
      topics: topics,
    })
    this.logger.info(`Created topic: ${topic}`)
    await this.admin.disconnect()
  }

  async sendMessage(topic: string, message: Array<Object> | Object | string, headers?: IHeaders) {
    if (!headers) {
      headers = {}
    }
    let messages: Message[] = []
    if (typeof message === 'object') {
      if (Array.isArray(message)) {
        message.forEach((msg) => {
          messages.push({ headers, value: JSON.stringify(msg) })
        })
      } else {
        messages.push({ headers, value: JSON.stringify(message) })
      }
    } else {
      messages.push({ headers, value: message })
    }

    const producer = this.kafka.producer()
    await producer.connect()
    const record = await producer.send({ topic, messages })

    this.logger.info(`Sent successfully topic:${topic}`, { ...record })
    await producer.disconnect()
    return record
  }

  async consumeMessages(topic: string, callback: MessageCallback) {
    const consumer = this.kafka.consumer({ groupId: 'test-group' })
    await consumer.connect()
    await consumer.subscribe({ topics: topic.split(','), fromBeginning: true })

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const { headers, offset, value } = message
        const payload = value?.toString() ?? ''

        for (const key in headers) {
          if (headers?.hasOwnProperty(key) && Buffer.isBuffer(headers[key])) {
            headers[key] = headers[key]?.toString()
          }
        }
        this.logger.info('received message', {
          topic,
          partition,
          offset,
          value: payload,
          headers: headers,
        })
        callback(topic, payload)
      },
    })
  }
}
