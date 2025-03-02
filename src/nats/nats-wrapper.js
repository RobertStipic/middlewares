import { connect } from "nats";
import pkg from "./subjects/subjects.js";
const { Subjects } = pkg;

export class NatsWrapper {
  _client;
  _jsClient;

  get client() {
    if (!this._client) {
      throw new Error("Cannot access NATS client before connecting.");
    }
    return this._client;
  }

  get jsClient() {
    if (!this._jsClient) {
      throw new Error("Cannot access JetStream client before connecting.");
    }
    return this._jsClient;
  }

  async connect(url) {
    try {
      this._client = await connect({ servers: [url] });
      const subjects = Object.values(Subjects).map(
        (subject) => `vodapp.${subject}`
      );
      await this.createStreamIfNotExists("VODAPP", subjects);
      this._jsClient = this.client.jetstream();
      console.log("Successfully connected to NATS and initialized JetStream.");
    } catch (err) {
      console.error("Error in NATS connection: ", err);
      throw err;
    }
  }

  async createStreamIfNotExists(streamName, subjects) {
    const jsm = await this.client.jetstreamManager();
    const streams = await jsm.streams.list().next();
    const streamExists = streams.some(
      (stream) => stream.config.name === streamName
    );

    if (!streamExists) {
      await jsm.streams.add({
        name: streamName,
        subjects: subjects,
      });
      console.log(`Stream ${streamName} created.`);
    } else {
      console.log(`Stream ${streamName} already exists.`);
    }
  }

  close() {
    if (this._client) {
      this._client.close();
      console.log("NATS connection closed.");
    }
  }
}
