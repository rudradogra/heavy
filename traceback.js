import amqp from 'amqplib';
import { randomUUID } from 'crypto';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const RABBITMQ_URL = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672';
const SERVICE_NAME = process.env.TRACEBACK_SERVICE_NAME ?? 'heavy';
const TRACE_QUEUE = process.env.TRACE_QUEUE ?? 'logs.ingest';

let rabbitChannel = null;

// ---------------------------------------------------------------------------
// RabbitMQ connection
// ---------------------------------------------------------------------------

/**
 * Connect to RabbitMQ and assert the Traceback ingest queue.
 * Fails gracefully — telemetry falls back to console-only if offline.
 */
export async function initTraceback() {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();
    rabbitChannel = channel;
    await channel.assertQueue(TRACE_QUEUE, { durable: true });
    console.log(`[traceback] queue "${TRACE_QUEUE}" ready (service=${SERVICE_NAME})`);
  } catch {
    rabbitChannel = null;
    console.warn('[traceback] RabbitMQ offline — telemetry logs to console only');
  }
}

// ---------------------------------------------------------------------------
// Telemetry publishing
// ---------------------------------------------------------------------------

function buildMessage(message, context) {
  if (!context || typeof context !== 'object') {
    return message;
  }
  return `${message}\n${JSON.stringify(context, null, 2)}`;
}

/**
 * Publish an observability signal to Traceback via RabbitMQ.
 *
 * @param {object} opts
 * @param {'ERROR'|'WARN'|'INFO'|'FATAL'} opts.level
 * @param {string} opts.message - Primary log text (Traceback embeds this)
 * @param {string} [opts.traceId] - Correlation ID; auto-generated if omitted
 * @param {object} [opts.context] - Optional structured data appended as JSON
 */
export async function publishTelemetry({ level, message, traceId, context }) {
  const payload = {
    timestamp: new Date().toISOString(),
    log_level: level,
    service_name: SERVICE_NAME,
    trace_id: traceId ?? randomUUID(),
    message: buildMessage(message, context),
  };

  console.error('[telemetry]', JSON.stringify(payload));

  if (!rabbitChannel) {
    return;
  }

  try {
    rabbitChannel.sendToQueue(TRACE_QUEUE, Buffer.from(JSON.stringify(payload)), {
      persistent: true,
    });
  } catch (err) {
    console.warn('[traceback] publish failed:', err.message);
  }
}
