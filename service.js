const api = require('@opentelemetry/api');
const tracer = require('./tracing')('service');
const express = require("express");
const PORT = "8080";
const app = express();
const { Kafka, logLevel } = require('kafkajs')
const kafka = new Kafka({
  logLevel: logLevel.ERROR,
  brokers: [`localhost:9092`],
  clientId: 'example-producer',
})
const producer = kafka.producer()
const topic = 'report'
const { W3CTraceContextPropagator } = require('@opentelemetry/core');

async function report(parentSpan) {
  try {
    const ctx = api.trace.setSpan(
      api.context.active(),
      parentSpan
    );
    let span = tracer.startSpan('report_producer', undefined, ctx);
    const carrier = {};
    const propagator = new W3CTraceContextPropagator();
    propagator.inject(
      api.trace.setSpanContext(api.context.active(), span.spanContext()),
      carrier,
      api.defaultTextMapSetter,
    );
    await producer.connect()
    await producer
      .send({
        topic,
        messages: Array({
          key: `report`,
          value: JSON.stringify(carrier)
        })
      });
    span.end()
  } catch (e) {
    console.log(e)
    throw e
  }
}
app.get("/api/report", async (req, res) => {
  try {
    let span = tracer.startSpan('report_request');
    let result = await report(span)
    span.end()
    res.send(result);
  } catch (e) {
    res.send(e);
  }
});

app.listen(parseInt(PORT, 10), () => {
  console.log(`Service listening for requests on http://localhost:${PORT}`);
});