import { Client } from "../transport/client"
import { Broadcast, BroadcastConfig } from "../contribute"
import { Connection } from "../transport/connection"
import type { TransportSessionFactory } from "../transport/session"

export { QmuxSession, createQmuxTransportSessionFactory, qmuxTransportSessionFactory } from "../transport/qmux"
export type { QmuxSessionOptions } from "../transport/qmux"
export type { TransportSessionFactory } from "../transport/session"

export interface PublisherOptions {
	url: string
	namespace: string[]
	media: MediaStream
	video?: VideoEncoderConfig
	audio?: AudioEncoderConfig
	fingerprintUrl?: string
	sessionFactory?: TransportSessionFactory
}

export class PublisherApi {
	private client: Client
	private connection?: Connection
	private broadcast?: Broadcast
	private opts: PublisherOptions

	constructor(opts: PublisherOptions) {
		this.opts = opts
		this.client = new Client({
			url: opts.url,
			fingerprint: opts.fingerprintUrl,
			sessionFactory: opts.sessionFactory,
		})
	}

	async publish(): Promise<void> {
		if (!this.connection) {
			this.connection = await this.client.connect()
		}

		const bcConfig: BroadcastConfig = {
			connection: this.connection,
			namespace: this.opts.namespace,
			media: this.opts.media,
			video: this.opts.video,
			audio: this.opts.audio,
		}

		this.broadcast = new Broadcast(bcConfig)
	}

	async stop(): Promise<void> {
		if (this.broadcast) {
			this.broadcast.close()
			await this.broadcast.closed()
		}
		if (this.connection) {
			this.connection.close()
			await this.connection.closed()
		}
	}
}
