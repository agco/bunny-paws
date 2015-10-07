# Bunny Paws

Handy tool to pause/resume RabbitMQ consumers. You simply need to publish "pause"/"resume" message to control exchange and all queues managed by
Bunny Paws will be paused or resumed.


    var queueA = consumingRabbit.default().queue({ name: queueNameA });
    var queueB = consumingRabbit.default().queue({ name: queueNameB });
    bunnyPaws.addPauseResume(queueA.consume(consumerSpyA));
    bunnyPaws.pause();
    bunnyPaws.resume();

BunnPaws let's you quickly add routes to your express app to control each queue state:

    app = express();
    server = app.listen(config.port);
    baseUrl = 'http://localhost:' + config.port + '/api';
    bunnyPawsModule.registerRoutes(app, baseUrl, connectionUrl, [amqpHttpApiPort], [amqpHttpApiProtocol]);

now you can pause/resume specific queue:

    POST /queues/:queueName/pause
    POST /queues/:queueName/resume

or pause/resume all managed queues:

    POST /queues/pause
    POST /queues/resume

