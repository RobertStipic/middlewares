export class Responder {
  constructor(natsClient, subject, queueGroupName) {
    this.natsClient = natsClient;
    this.subject = subject;
    this.queueGroupName = queueGroupName;
    this.subscription = null;

    if (!this.subject) throw new Error("Subject must be defined");
    if (!this.queueGroupName) throw new Error("Queue group must be defined");
    if (!this.onMessage) throw new Error("onMessage handler must be defined");
  }

  listen() {
    this.subscription = this.natsClient.subscribe(this.subject, {
      queue: this.queueGroupName,
      callback: async (err, msg) => {
        if (err) {
          console.error(`[Responder] Error on ${this.subject}:`, err);
          return;
        }

        try {
          const data = JSON.parse(msg.string());
          await this.onMessage(data, msg);
        } catch (error) {
          console.error(`[Responder] Handler error on ${this.subject}:`, error);
          if (msg.reply) {
            msg.respond(JSON.stringify({ error: error.message }));
          }
        }
      },
    });

    console.log(
      `Responder listening for ${this.subject} / ${this.queueGroupName}`,
    );
  }

  stop() {
    if (this.subscription) {
      this.subscription.unsubscribe();
      console.log(`Responder stopped for ${this.subject}`);
    }
  }
}
