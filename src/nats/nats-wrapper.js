import { connect, StringCodec, JSONCodec } from "nats";
import pkg from "./subjects/subjects.js";
const { Subjects } = pkg;

export class NatsWrapper {
  _client;
  _jsClient;
  _jsonCodec = JSONCodec();
  _stringCodec = StringCodec();
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
    get jsonCodec() {
    return this._jsonCodec;
  }

  get stringCodec() {
    return this._stringCodec;
  }

  async connect(url) {
    try {
      this._client = await connect({ servers: [url] });
      const subjects = Object.values(Subjects).map(
        (subject) => `vodapp.${subject}`
      );
      await this.createStreamIfNotExists(Subjects.StreamName, subjects);
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
    
  async jetstreamRequest(subject, data, options = {}) {
    if (!this._jsClient) {
      throw new Error("JetStream client not connected");
    }

    const timeout = options.timeout || 15000;
    
    try {
      const response = await this._client.request(
        subject,
        this.jsonCodec.encode(data),
        { timeout }
      );
      
      return this.jsonCodec.decode(response.data);
    } catch (error) {
      console.error("JetStream request failed:", error);
      throw error;
    }
  }

    
  async createConsumer(streamName, durableName, subjectFilter) {
    const jsm = await this.client.jetstreamManager();
    
    try {
      await jsm.consumers.add(streamName, {
        durable_name: durableName,
        ack_policy: "explicit",
        filter_subject: subjectFilter,
        deliver_policy: "all",
        ack_wait: 30000,
      });
      console.log(`Consumer ${durableName} created for stream ${streamName} listening for for subject ${subjectFilter}`);
    } catch (err) {
      if (err.code === '10058') { 
        console.log(`Consumer ${durableName} already exists`);
      } 
    }
  }
  close() {
    if (this._client) {
      this._client.close();
      console.log("NATS connection closed.");
    }
  }
}
