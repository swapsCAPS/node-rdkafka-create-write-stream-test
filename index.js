const os = require('os')
const async = require('async')
const fs = require('fs')
const util = require('util')
const Kafka = require('node-rdkafka')
const split2 = require('split2')
const nodeStream = require('stream')

const { Writable } = nodeStream

const appendFile = util.promisify(fs.appendFile)
const unlink = util.promisify(fs.unlink)
const pipeline = util.promisify(nodeStream.pipeline)

const fileName = './msgs'

process.once('unhandledRejection', (rejection) => {
  console.log(rejection)
  throw rejection
})

const totalMsgs = 999999

const generate = async () => {
  for (var i = 0; i < totalMsgs; i++) {
    await appendFile(fileName, `${i} ${'❤'.repeat(100)}\n`)
  }
}

const run = async () => {
  await unlink(fileName).catch((error) => console.warn(error))

  console.log(`generating test msgs to ${fileName}`)

  await generate()

  const globalConfig = {
    'metadata.broker.list': process.env.KAFKA_BROKER_LIST || 'localhost:9092'
  }
  const consumerConfig = {
    'group.id': os.hostname()
  }
  const topicConfig = {
    'auto.offset.reset': 'earliest',
  }

  const topic = `create-write-stream-test`

  const producerStream = Kafka.Producer.createWriteStream(globalConfig, {}, { topic });

  const client = Kafka.AdminClient.create({
    ...globalConfig,
    'client.id': 'kafka-admin',
  })

  await async.series([
    (cb) => {
      console.log('deleting topic')
      client.deleteTopic(topic, 5000, (error) => {
        if (error) {
          console.error('Could not delete topic', error)
          if (error.code === 3) return cb()
          return cb(error)
        }
        setTimeout(cb, 10000)
      })
    },
    (cb) => {
      console.log('creating topic')
      client.createTopic({
        topic,
        num_partitions:     100,
        replication_factor: 1,
      }, cb)
    },
  ]).catch(error => {
    throw new Error(`Something went wrong ${error.message}`)
  })

  const streams = [
    fs.createReadStream(fileName),
    split2(),
    producerStream,
  ]

  let count = 0

  console.log('Producing messages to Kafka...')

  pipeline(...streams)
    .then(() => {
      console.log('Done producing')
    }).catch((error) => {
      console.error(error)
    })
    .then(() => {
      const consumerStream = Kafka.KafkaConsumer.createReadStream({ ...globalConfig, ...consumerConfig }, topicConfig, { topics: [ topic ] })
      const writable = new Writable({
          objectMode: true,
          write: (obj, enc, cb) => {
            count++
            console.log('obj', obj.value.toString(), count)
            if (count === totalMsgs) consumerStream.push(null)
            cb()
          }
        })

      return pipeline(consumerStream, writable)
    })
    .then(() => {
      console.log(`Done. Produced: ${count}`)
    }).catch((error) => {
      console.error(error)
      console.error(`Produced: ${count}`)
    })
}

run()
