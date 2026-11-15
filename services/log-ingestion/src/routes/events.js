import { eventRequestSchema } from '../schemas/event.js';
import { buildEvent } from '../services/event-builder.js';
import { publishEvent } from '../services/kafka.js';

export async function eventRoutes(app) {
  app.post('/events', async (request, reply) => {
    const validation = eventRequestSchema.safeParse(request.body);

    if (!validation.success) {
      return reply.code(400).send({
        error: 'Invalid event',
        details: validation.error.issues
      });
    }

    let event;

    try {
      event = buildEvent(validation.data);
    } catch (error) {
      if (error.statusCode === 413) {
        return reply.code(413).send({
          error: error.message
        });
      }

      request.log.error(error);

      return reply.code(500).send({
        error: 'Could not build event'
      });
    }

    try {
      await publishEvent(event);

      return reply.code(202).send({
        eventId: event.eventId,
        status: 'published'
      });
    } catch (error) {
      request.log.error(error);

      return reply.code(503).send({
        error: 'Event could not be published'
      });
    }
  });
}
