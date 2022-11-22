const api = require('@opentelemetry/api');
const tracer = require('./tracing')('consumer');
const { W3CTraceContextPropagator } = require('@opentelemetry/core');
const { Kafka, logLevel } = require('kafkajs')
const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`localhost:9092`],
  clientId: 'example-consumer',
})
const topic = 'report'
const consumer = kafka.consumer({ groupId: 'test-group' })

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log("topic:" + topic + "  message:" + message.value)
      const carrier = JSON.parse(message.value)
      const propagator = new W3CTraceContextPropagator();
      const parentCtx = propagator.extract(api.context.active(), carrier, api.defaultTextMapGetter);
      if (topic === "report") {
        const span = tracer.startSpan(`report_consumer`, undefined, parentCtx);
        report(span)
        span.end()
      }
    },
  })
}

function report(parentSpan) {
  const parentCtx = api.trace.setSpan(
    api.context.active(),
    parentSpan
  );
  let span = tracer.startSpan('handle_report', undefined, parentCtx);
  for (let i = 0; i < 10; i++) {
    doWork(span, i);
  }
  span.end();
}
function doWork(parentSpan, i) {
  const parentCtx = api.trace.setSpan(
    api.context.active(),
    parentSpan
  );
  let span = tracer.startSpan(`doWork:${i}`, undefined, parentCtx);
  // 讓每個執行的時間不一樣
  for (let i = 0; i <= Math.floor(Math.random() * 40000000); i++) {
    // empty
  }
  if (i === 5) { // 產生錯誤訊息
    span.recordException(new Error(`doWork:${i} error`));
    span.setStatus({ code: api.SpanStatusCode.ERROR, message: "Something wrong!" });
    span.addEvent('log', {
      'log.severity': 'error',
      'log.message': 'doWork:${i} error'
    });
  }
  span.end();
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']
// 收到無法處理錯誤時的處理
errorTypes.forEach(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})
// 收到中斷訊號時的處理
signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})