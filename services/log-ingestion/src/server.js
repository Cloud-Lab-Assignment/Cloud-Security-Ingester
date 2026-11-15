import Fastify from 'fastify';
import { env } from './config/env.js';
import {
  connectKafka,
  checkKafka,
  disconnectKafka
} from './services/kafka.js';
import { eventRoutes } from './routes/events.js';

const app = Fastify({
  logger: true,
  bodyLimit: 1024 * 1024
});

app.get('/health', async () => {
  return {
    status: 'ok',
    service: env.KAFKA_CLIENT_ID
  };
});

app.get('/ready', async (request, reply) => {
  try {
    await checkKafka();

    return {
      status: 'ready',
      kafka: 'connected'
    };
  } catch (error) {
    request.log.error(error);

    return reply.code(503).send({
      status: 'not_ready',
      kafka: 'disconnected'
    });
  }
});

app.register(eventRoutes);

async function start() {
  try {
    await connectKafka();

    await app.listen({
      port: env.PORT,
      host: env.HOST
    });
  } catch (error) {
    app.log.error(error);
    process.exit(1);
  }
}

async function shutdown() {
  try {
    await app.close();
    await disconnectKafka();
    process.exit(0);
  } catch (error) {
    app.log.error(error);
    process.exit(1);
  }
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

start();
