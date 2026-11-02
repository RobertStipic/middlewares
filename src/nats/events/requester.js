export class Requester {
  constructor(natsClient, jsonCodec) {
    this.natsClient = natsClient;
    this.jsonCodec = jsonCodec;
    this.timeout = 10000;
  }

  async request(subject, data) {
    try {
      const response = await this.natsClient.request(
        subject,
        this.jsonCodec.encode(data),
        { timeout: this.timeout },
      );
      return this.jsonCodec.decode(response.data);
    } catch (error) {
      if (error.code === "503" || error.code === "TIMEOUT") {
        throw new Error(`Service unavailable or timeout for ${subject}`);
      }
      throw error;
    }
  }
}
