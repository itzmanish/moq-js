export interface TransportBidirectionalStream {
	readable: ReadableStream<Uint8Array>
	writable: WritableStream<Uint8Array>
}

export interface TransportDatagramDuplexStream {
	readable: ReadableStream<Uint8Array>
	writable: WritableStream<Uint8Array>
}

export interface TransportStreamOptions {
	sendOrder?: number
}

export interface TransportCertificateHash {
	algorithm: string
	value: BufferSource
}

export interface TransportSessionOptions {
	serverCertificateHashes?: TransportCertificateHash[]
}

export interface TransportCloseInfo {
	closeCode?: number
	reason?: string
}

export type TransportSessionFactory = (url: string, options: TransportSessionOptions) => TransportSession

export interface TransportSession {
	readonly ready: Promise<void>
	readonly datagrams?: TransportDatagramDuplexStream
	readonly incomingUnidirectionalStreams: ReadableStream<ReadableStream<Uint8Array>>

	createBidirectionalStream(options?: TransportStreamOptions): Promise<TransportBidirectionalStream>
	createUnidirectionalStream(options?: TransportStreamOptions): Promise<WritableStream<Uint8Array>>
	close(closeInfo?: TransportCloseInfo): void
}

interface NativeWebTransportSession extends TransportSession {
	readonly datagrams: TransportDatagramDuplexStream
}

type NativeWebTransportConstructor = new (url: string, options?: TransportSessionOptions) => NativeWebTransportSession

export class WebTransportSession implements TransportSession {
	#inner: NativeWebTransportSession

	constructor(url: string, options: TransportSessionOptions = {}) {
		const WebTransportConstructor = (
			globalThis as typeof globalThis & { WebTransport?: NativeWebTransportConstructor }
		).WebTransport
		if (!WebTransportConstructor) {
			throw new Error("WebTransport is not available")
		}

		this.#inner = new WebTransportConstructor(url, options)
	}

	get ready(): Promise<void> {
		return this.#inner.ready
	}

	get datagrams(): TransportDatagramDuplexStream {
		return this.#inner.datagrams
	}

	get incomingUnidirectionalStreams(): ReadableStream<ReadableStream<Uint8Array>> {
		return this.#inner.incomingUnidirectionalStreams
	}

	createBidirectionalStream(options?: TransportStreamOptions): Promise<TransportBidirectionalStream> {
		return this.#inner.createBidirectionalStream(options)
	}

	createUnidirectionalStream(options?: TransportStreamOptions): Promise<WritableStream<Uint8Array>> {
		return this.#inner.createUnidirectionalStream(options)
	}

	close(closeInfo?: TransportCloseInfo): void {
		this.#inner.close(closeInfo)
	}
}

export const defaultTransportSessionFactory: TransportSessionFactory = (url, options) =>
	new WebTransportSession(url, options)
