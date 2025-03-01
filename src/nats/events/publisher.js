export class Publisher {
  constructor(jsClient, subject) {
    this.jsClient = jsClient;
    this.subject = subject;
    if (!this.subject) {
      throw new Error("Subject must be defined in child class");
    }
  }

  async publish(data) {
    await this.jsClient.publish(`vodapp.${this.subject}`, JSON.stringify(data));
    console.log(
      "Event published to:",
      this.subject
      /* " with following data:",
      data*/
    );
  }
}
