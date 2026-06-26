import type {
	TransportBidirectionalStream,
	TransportCloseInfo,
	TransportSession,
	TransportSessionFactory,
	TransportSessionOptions,
	TransportStreamOptions,
} from "./session"

const DEFAULT_MAX_RECORD_SIZE = 16382
const DEFAULT_MAX_STREAMS = 100000
const DEFAULT_MAX_DATA = Number.MAX_SAFE_INTEGER
const SEND_BUFFER_HIGH_WATER_MARK = 1024 * 1024

const FRAME_RESET_STREAM = 0x04n
const FRAME_STOP_SENDING = 0x05n
const FRAME_STREAM_MIN = 0x08n
const FRAME_STREAM_MAX = 0x0fn
const FRAME_MAX_DATA = 0x10n
const FRAME_MAX_STREAM_DATA = 0x11n
const FRAME_MAX_STREAMS_BIDI = 0x12n
const FRAME_MAX_STREAMS_UNI = 0x13n
const FRAME_DATA_BLOCKED = 0x14n
const FRAME_STREAM_DATA_BLOCKED = 0x15n
const FRAME_STREAMS_BLOCKED_BIDI = 0x16n
const FRAME_STREAMS_BLOCKED_UNI = 0x17n
const FRAME_CONNECTION_CLOSE = 0x1cn
const FRAME_CONNECTION_CLOSE_APP = 0x1dn
const FRAME_QX_TRANSPORT_PARAMETERS = 0x3f5153300d0a0d0an
const FRAME_QX_PING = 0x348c67529ef8c7bdn
const FRAME_QX_PING_RESPONSE = 0x348c67529ef8c7ben

const TP_INITIAL_MAX_DATA = 0x04n
const TP_INITIAL_MAX_STREAM_DATA_BIDI_LOCAL = 0x05n
const TP_INITIAL_MAX_STREAM_DATA_BIDI_REMOTE = 0x06n
const TP_INITIAL_MAX_STREAM_DATA_UNI = 0x07n
const TP_INITIAL_MAX_STREAMS_BIDI = 0x08n
const TP_INITIAL_MAX_STREAMS_UNI = 0x09n
const TP_MAX_RECORD_SIZE = 0x0571c59429cd0845n

type QmuxRole = "client" | "server"

interface QmuxWebSocket extends EventTarget {
	binaryType?: BinaryType
	readonly readyState: number
	readonly bufferedAmount?: number
	send(data: BufferSource): void
	close(code?: number, reason?: string): void
}

type QmuxWebSocketConstructor = new (url: string, protocols?: string | string[]) => QmuxWebSocket

export interface QmuxSessionOptions extends TransportSessionOptions {
	role?: QmuxRole
	protocols?: string | string[]
	WebSocket?: QmuxWebSocketConstructor
	maxRecordSize?: number
}

type StreamMode = "bidi" | "send-uni" | "recv-uni"

interface QmuxStreamEntry {
	stream: QmuxStream
	mode: StreamMode
}

interface VarIntRead {
	value: bigint
	bytes: number
}

interface Cursor {
	buffer: Uint8Array
	offset: number
}

export class QmuxSession implements TransportSession {
	#role: QmuxRole
	#socket: QmuxWebSocket
	#incomingUnidirectionalStreams: ReadableStream<ReadableStream<Uint8Array>>
	#incomingUnidirectionalController?: ReadableStreamDefaultController<ReadableStream<Uint8Array>>
	#streams = new Map<number, QmuxStreamEntry>()
	#rxBuffer: Uint8Array = new Uint8Array()
	#ready: Promise<void>
	#resolveReady!: () => void
	#rejectReady!: (err: unknown) => void
	#readySettled = false
	#receivedTransportParameters = false
	#closed = false
	#peerMaxRecordSize = DEFAULT_MAX_RECORD_SIZE
	#localMaxRecordSize: number
	#nextBidiStreamId: number
	#nextUniStreamId: number

	constructor(url: string, options: QmuxSessionOptions = {}) {
		this.#role = options.role ?? "client"
		this.#localMaxRecordSize = options.maxRecordSize ?? DEFAULT_MAX_RECORD_SIZE
		if (this.#localMaxRecordSize < DEFAULT_MAX_RECORD_SIZE) {
			throw new Error("QMux maxRecordSize must be at least 16382")
		}

		this.#nextBidiStreamId = this.#role === "client" ? 0 : 1
		this.#nextUniStreamId = this.#role === "client" ? 2 : 3

		this.#ready = new Promise((resolve, reject) => {
			this.#resolveReady = resolve
			this.#rejectReady = reject
		})

		this.#incomingUnidirectionalStreams = new ReadableStream<ReadableStream<Uint8Array>>({
			start: (controller) => {
				this.#incomingUnidirectionalController = controller
			},
		})

		const WebSocketConstructor = options.WebSocket ?? getWebSocketConstructor()
		this.#socket = new WebSocketConstructor(toWebSocketUrl(url), options.protocols)
		this.#socket.binaryType = "arraybuffer"
		this.#socket.addEventListener("open", () => {
			this.#sendTransportParameters().catch((err) => this.#fail(err))
		})
		this.#socket.addEventListener("message", (event) => {
			this.#handleMessage(event as MessageEvent).catch((err) => this.#fail(err))
		})
		this.#socket.addEventListener("error", () => {
			this.#fail(new Error("QMux WebSocket error"))
		})
		this.#socket.addEventListener("close", () => {
			this.#finish()
		})
	}

	get ready(): Promise<void> {
		return this.#ready
	}

	get incomingUnidirectionalStreams(): ReadableStream<ReadableStream<Uint8Array>> {
		return this.#incomingUnidirectionalStreams
	}

	async createBidirectionalStream(_options?: TransportStreamOptions): Promise<TransportBidirectionalStream> {
		await this.#ready
		const stream = this.#createLocalStream(this.#nextBidiStreamId, "bidi")
		this.#nextBidiStreamId += 4
		return {
			readable: stream.readable,
			writable: stream.writable,
		}
	}

	async createUnidirectionalStream(_options?: TransportStreamOptions): Promise<WritableStream<Uint8Array>> {
		await this.#ready
		const stream = this.#createLocalStream(this.#nextUniStreamId, "send-uni")
		this.#nextUniStreamId += 4
		return stream.writable
	}

	close(closeInfo?: TransportCloseInfo): void {
		if (this.#closed) return

		try {
			if (this.#socket.readyState === 1) {
				const frame = encodeConnectionCloseFrame(BigInt(closeInfo?.closeCode ?? 0), closeInfo?.reason ?? "")
				this.#socket.send(encodeRecord(frame))
			}
		} catch {
			// The underlying WebSocket may already be closing.
		}

		this.#finish()
		try {
			this.#socket.close(1000, webSocketReason(closeInfo?.reason ?? ""))
		} catch {
			// Ignore invalid states during local shutdown.
		}
	}

	async sendStreamData(streamId: number, streamOffset: bigint, data: Uint8Array, fin: boolean): Promise<void> {
		if (this.#closed) throw new Error("QMux session is closed")

		if (data.byteLength === 0) {
			if (fin) await this.#sendFrame(encodeStreamFrame(streamId, streamOffset, data, true))
			return
		}

		const maxPayload = Math.max(1, this.#peerMaxRecordSize - 16)
		let offset = 0
		while (offset < data.byteLength) {
			const end = Math.min(offset + maxPayload, data.byteLength)
			const chunk = data.subarray(offset, end)
			await this.#sendFrame(
				encodeStreamFrame(streamId, streamOffset + BigInt(offset), chunk, fin && end === data.byteLength),
			)
			offset = end
		}
	}

	finishSendingStream(streamId: number): void {
		const entry = this.#streams.get(streamId)
		if (entry?.mode === "send-uni") this.#streams.delete(streamId)
	}

	#createLocalStream(streamId: number, mode: StreamMode): QmuxStream {
		const stream = new QmuxStream(this, streamId)
		this.#streams.set(streamId, { stream, mode })
		return stream
	}

	async #sendTransportParameters(): Promise<void> {
		await this.#sendFrame(encodeTransportParametersFrame(this.#localMaxRecordSize))
	}

	async #sendFrame(frame: Uint8Array): Promise<void> {
		if (frame.byteLength > this.#peerMaxRecordSize) {
			throw new Error(`QMux frame exceeds peer max record size: ${frame.byteLength}`)
		}
		if (this.#socket.readyState !== 1) {
			throw new Error("QMux WebSocket is not open")
		}

		await this.#waitForWritable()
		this.#socket.send(encodeRecord(frame))
	}

	async #waitForWritable(): Promise<void> {
		while (
			!this.#closed &&
			this.#socket.readyState === 1 &&
			(this.#socket.bufferedAmount ?? 0) > SEND_BUFFER_HIGH_WATER_MARK
		) {
			await new Promise((resolve) => setTimeout(resolve, 4))
		}
	}

	async #handleMessage(event: MessageEvent): Promise<void> {
		const data = await messageDataToBytes(event.data)
		this.#rxBuffer = concatBytes(this.#rxBuffer, data)

		for (;;) {
			const size = tryReadVarInt(this.#rxBuffer, 0)
			if (!size) return
			const recordSize = toSafeNumber(size.value, "QMux record size")
			const recordStart = size.bytes
			const recordEnd = recordStart + recordSize
			if (this.#rxBuffer.byteLength < recordEnd) return

			const record = this.#rxBuffer.slice(recordStart, recordEnd)
			this.#rxBuffer = this.#rxBuffer.slice(recordEnd)
			this.#handleRecord(record)
		}
	}

	#handleRecord(record: Uint8Array): void {
		if (record.byteLength > this.#localMaxRecordSize) {
			throw new Error(`QMux record exceeds local max record size: ${record.byteLength}`)
		}

		const cursor: Cursor = { buffer: record, offset: 0 }
		while (cursor.offset < cursor.buffer.byteLength) {
			const frameStart = cursor.offset
			const frameType = readVarInt(cursor)

			if (!this.#receivedTransportParameters) {
				if (frameType !== FRAME_QX_TRANSPORT_PARAMETERS) {
					throw new Error("QMux peer did not send transport parameters first")
				}
				this.#handleTransportParameters(cursor)
				this.#receivedTransportParameters = true
				this.#settleReady()
				continue
			}

			if (frameType === FRAME_QX_TRANSPORT_PARAMETERS) {
				throw new Error("QMux peer sent duplicate transport parameters")
			}

			this.#handleFrame(frameType, cursor, frameStart)
		}
	}

	#handleFrame(frameType: bigint, cursor: Cursor, frameStart: number): void {
		if (frameType >= FRAME_STREAM_MIN && frameType <= FRAME_STREAM_MAX) {
			this.#handleStreamFrame(Number(frameType), cursor)
			return
		}

		switch (frameType) {
			case FRAME_RESET_STREAM:
				this.#handleResetStream(cursor)
				return
			case FRAME_STOP_SENDING:
				readVarInt(cursor)
				readVarInt(cursor)
				return
			case FRAME_MAX_DATA:
			case FRAME_MAX_STREAMS_BIDI:
			case FRAME_MAX_STREAMS_UNI:
			case FRAME_DATA_BLOCKED:
			case FRAME_STREAMS_BLOCKED_BIDI:
			case FRAME_STREAMS_BLOCKED_UNI:
				readVarInt(cursor)
				return
			case FRAME_MAX_STREAM_DATA:
			case FRAME_STREAM_DATA_BLOCKED:
				readVarInt(cursor)
				readVarInt(cursor)
				return
			case FRAME_CONNECTION_CLOSE:
				readVarInt(cursor)
				readVarInt(cursor)
				readVarBytes(cursor)
				this.#finish()
				return
			case FRAME_CONNECTION_CLOSE_APP:
				readVarInt(cursor)
				readVarBytes(cursor)
				this.#finish()
				return
			case FRAME_QX_PING:
				this.#sendFrame(encodePingFrame(FRAME_QX_PING_RESPONSE, readVarInt(cursor))).catch((err) =>
					this.#fail(err),
				)
				return
			case FRAME_QX_PING_RESPONSE:
				readVarInt(cursor)
				return
			default:
				throw new Error(`unsupported QMux frame type: ${frameType} at offset ${frameStart}`)
		}
	}

	#handleTransportParameters(cursor: Cursor): void {
		const payload = readVarBytes(cursor)
		const params: Cursor = { buffer: payload, offset: 0 }

		while (params.offset < params.buffer.byteLength) {
			const id = readVarInt(params)
			const value = readVarBytes(params)

			if (id === TP_MAX_RECORD_SIZE) {
				const maxRecordSize = toSafeNumber(readVarInt({ buffer: value, offset: 0 }), "QMux max_record_size")
				if (maxRecordSize < DEFAULT_MAX_RECORD_SIZE) {
					throw new Error("QMux peer advertised invalid max_record_size")
				}
				this.#peerMaxRecordSize = maxRecordSize
			}
		}
	}

	#handleStreamFrame(frameType: number, cursor: Cursor): void {
		const streamId = toSafeNumber(readVarInt(cursor), "QMux stream id")
		const hasOffset = (frameType & 0x04) !== 0
		const hasLength = (frameType & 0x02) !== 0
		const hasFin = (frameType & 0x01) !== 0

		const streamOffset = hasOffset ? readVarInt(cursor) : 0n

		const payloadLength = hasLength
			? toSafeNumber(readVarInt(cursor), "QMux STREAM length")
			: cursor.buffer.byteLength - cursor.offset
		const payload = readBytes(cursor, payloadLength)

		const entry = this.#streams.get(streamId) ?? this.#createIncomingStream(streamId)
		if (entry.mode === "send-uni") {
			throw new Error("QMux peer sent data on a local unidirectional stream")
		}

		entry.stream.receive(payload, hasFin, streamOffset)
		if (hasFin && entry.mode === "recv-uni") {
			this.#streams.delete(streamId)
		}
	}

	#handleResetStream(cursor: Cursor): void {
		const streamId = toSafeNumber(readVarInt(cursor), "QMux stream id")
		readVarInt(cursor)
		readVarInt(cursor)

		const entry = this.#streams.get(streamId)
		if (entry) {
			entry.stream.error(new Error("QMux stream reset"))
			this.#streams.delete(streamId)
		}
	}

	#createIncomingStream(streamId: number): QmuxStreamEntry {
		if (!this.#isRemoteInitiated(streamId)) {
			throw new Error(`QMux received data for unknown local stream: ${streamId}`)
		}
		if (!isUnidirectional(streamId)) {
			throw new Error("QMux peer-initiated bidirectional streams are not supported")
		}

		const stream = new QmuxStream(this, streamId)
		const entry: QmuxStreamEntry = { stream, mode: "recv-uni" }
		this.#streams.set(streamId, entry)
		this.#incomingUnidirectionalController?.enqueue(stream.readable)
		return entry
	}

	#isRemoteInitiated(streamId: number): boolean {
		const localInitiator = this.#role === "client" ? 0 : 1
		return (streamId & 0x01) !== localInitiator
	}

	#settleReady(): void {
		if (this.#readySettled) return
		this.#readySettled = true
		this.#resolveReady()
	}

	#finish(): void {
		if (this.#closed) return
		this.#closed = true

		if (!this.#readySettled) {
			this.#readySettled = true
			this.#rejectReady(new Error("QMux session closed before ready"))
		}

		for (const entry of this.#streams.values()) {
			entry.stream.finish()
		}
		this.#streams.clear()
		this.#incomingUnidirectionalController?.close()
	}

	#fail(err: unknown): void {
		if (this.#closed) return
		const error = err instanceof Error ? err : new Error(String(err))
		this.#closed = true

		if (!this.#readySettled) {
			this.#readySettled = true
			this.#rejectReady(error)
		}

		for (const entry of this.#streams.values()) {
			entry.stream.error(error)
		}
		this.#streams.clear()
		this.#incomingUnidirectionalController?.error(error)

		try {
			this.#socket.close(1011, "QMux error")
		} catch {
			// Ignore invalid states during error shutdown.
		}
	}
}

class QmuxStream {
	readonly readable: ReadableStream<Uint8Array>
	readonly writable: WritableStream<Uint8Array>

	#controller?: ReadableStreamDefaultController<Uint8Array>
	#sendOffset = 0n
	#receiveOffset = 0n
	#readClosed = false

	constructor(
		private session: QmuxSession,
		private streamId: number,
	) {
		this.readable = new ReadableStream<Uint8Array>({
			start: (controller) => {
				this.#controller = controller
			},
		})
		this.writable = new WritableStream<Uint8Array>({
			write: async (chunk) => {
				const data = toUint8Array(chunk)
				const offset = this.#sendOffset
				await this.session.sendStreamData(this.streamId, offset, data, false)
				this.#sendOffset += BigInt(data.byteLength)
			},
			close: async () => {
				await this.session.sendStreamData(this.streamId, this.#sendOffset, new Uint8Array(), true)
				this.session.finishSendingStream(this.streamId)
			},
			abort: async () => {
				await this.session.sendStreamData(this.streamId, this.#sendOffset, new Uint8Array(), true)
				this.session.finishSendingStream(this.streamId)
			},
		})
	}

	receive(payload: Uint8Array, fin: boolean, streamOffset: bigint): void {
		if (this.#readClosed) return
		if (streamOffset !== this.#receiveOffset) throw new Error("QMux stream data arrived out of order")

		if (payload.byteLength > 0) this.#controller?.enqueue(payload)
		this.#receiveOffset += BigInt(payload.byteLength)
		if (fin) this.finish()
	}

	finish(): void {
		if (this.#readClosed) return
		this.#readClosed = true
		this.#controller?.close()
	}

	error(err: Error): void {
		if (this.#readClosed) return
		this.#readClosed = true
		this.#controller?.error(err)
	}
}

export const qmuxTransportSessionFactory: TransportSessionFactory = (url, options) => new QmuxSession(url, options)

export function createQmuxTransportSessionFactory(options: QmuxSessionOptions = {}): TransportSessionFactory {
	return (url, sessionOptions) => new QmuxSession(url, { ...sessionOptions, ...options })
}

function getWebSocketConstructor(): QmuxWebSocketConstructor {
	const WebSocketConstructor = (globalThis as typeof globalThis & { WebSocket?: QmuxWebSocketConstructor }).WebSocket
	if (!WebSocketConstructor) throw new Error("WebSocket is not available")
	return WebSocketConstructor
}

function toWebSocketUrl(url: string): string {
	if (url.startsWith("https://")) return `wss://${url.slice("https://".length)}`
	if (url.startsWith("http://")) return `ws://${url.slice("http://".length)}`
	return url
}

function encodeTransportParametersFrame(maxRecordSize: number): Uint8Array {
	const transportParameters = [
		encodeTransportParameter(TP_INITIAL_MAX_DATA, encodeVarInt(DEFAULT_MAX_DATA)),
		encodeTransportParameter(TP_INITIAL_MAX_STREAM_DATA_BIDI_LOCAL, encodeVarInt(DEFAULT_MAX_DATA)),
		encodeTransportParameter(TP_INITIAL_MAX_STREAM_DATA_BIDI_REMOTE, encodeVarInt(DEFAULT_MAX_DATA)),
		encodeTransportParameter(TP_INITIAL_MAX_STREAM_DATA_UNI, encodeVarInt(DEFAULT_MAX_DATA)),
		encodeTransportParameter(TP_INITIAL_MAX_STREAMS_BIDI, encodeVarInt(DEFAULT_MAX_STREAMS)),
		encodeTransportParameter(TP_INITIAL_MAX_STREAMS_UNI, encodeVarInt(DEFAULT_MAX_STREAMS)),
	]

	if (maxRecordSize > DEFAULT_MAX_RECORD_SIZE) {
		transportParameters.push(encodeTransportParameter(TP_MAX_RECORD_SIZE, encodeVarInt(maxRecordSize)))
	}

	const params = concatBytes(...transportParameters)

	return concatBytes(encodeVarInt(FRAME_QX_TRANSPORT_PARAMETERS), encodeVarInt(params.byteLength), params)
}

function encodeTransportParameter(id: bigint, value: Uint8Array): Uint8Array {
	return concatBytes(encodeVarInt(id), encodeVarInt(value.byteLength), value)
}

function encodeStreamFrame(streamId: number, streamOffset: bigint, payload: Uint8Array, fin: boolean): Uint8Array {
	const hasOffset = streamOffset > 0n
	const frameType = 0x08 | (hasOffset ? 0x04 : 0) | 0x02 | (fin ? 0x01 : 0)
	const offset = hasOffset ? encodeVarInt(streamOffset) : new Uint8Array()
	return concatBytes(
		encodeVarInt(frameType),
		encodeVarInt(streamId),
		offset,
		encodeVarInt(payload.byteLength),
		payload,
	)
}

function encodeConnectionCloseFrame(code: bigint, reason: string): Uint8Array {
	const reasonBytes = new TextEncoder().encode(reason)
	return concatBytes(
		encodeVarInt(FRAME_CONNECTION_CLOSE_APP),
		encodeVarInt(code),
		encodeVarInt(reasonBytes.byteLength),
		reasonBytes,
	)
}

function encodePingFrame(frameType: bigint, sequence: bigint): Uint8Array {
	return concatBytes(encodeVarInt(frameType), encodeVarInt(sequence))
}

function encodeRecord(frame: Uint8Array): Uint8Array {
	return concatBytes(encodeVarInt(frame.byteLength), frame)
}

function encodeVarInt(value: number | bigint): Uint8Array {
	const v = typeof value === "number" ? BigInt(value) : value
	if (v < 0n) throw new Error(`negative varint: ${value}`)

	let length: number
	let prefix: bigint
	if (v < 2n ** 6n) {
		length = 1
		prefix = 0n
	} else if (v < 2n ** 14n) {
		length = 2
		prefix = 0x40n
	} else if (v < 2n ** 30n) {
		length = 4
		prefix = 0x80n
	} else if (v < 2n ** 62n) {
		length = 8
		prefix = 0xc0n
	} else {
		throw new Error(`varint too large: ${value}`)
	}

	const bytes = new Uint8Array(length)
	for (let i = length - 1; i > 0; i -= 1) {
		bytes[i] = Number((v >> BigInt((length - 1 - i) * 8)) & 0xffn)
	}
	bytes[0] = Number((v >> BigInt((length - 1) * 8)) | prefix)
	return bytes
}

function tryReadVarInt(buffer: Uint8Array, offset: number): VarIntRead | undefined {
	if (offset >= buffer.byteLength) return undefined
	const first = buffer[offset]
	const prefix = first >> 6
	const bytes = 1 << prefix
	if (buffer.byteLength - offset < bytes) return undefined

	let value = BigInt(first & 0x3f)
	for (let i = 1; i < bytes; i += 1) {
		value = (value << 8n) | BigInt(buffer[offset + i])
	}

	return { value, bytes }
}

function readVarInt(cursor: Cursor): bigint {
	const result = tryReadVarInt(cursor.buffer, cursor.offset)
	if (!result) throw new Error("truncated varint")
	cursor.offset += result.bytes
	return result.value
}

function readBytes(cursor: Cursor, length: number): Uint8Array {
	if (length < 0 || cursor.offset + length > cursor.buffer.byteLength) throw new Error("truncated bytes")
	const bytes = cursor.buffer.slice(cursor.offset, cursor.offset + length)
	cursor.offset += length
	return bytes
}

function readVarBytes(cursor: Cursor): Uint8Array {
	return readBytes(cursor, toSafeNumber(readVarInt(cursor), "byte length"))
}

function toSafeNumber(value: bigint, context: string): number {
	if (value > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error(`${context} exceeds JavaScript safe integer range`)
	return Number(value)
}

function concatBytes(...chunks: Uint8Array[]): Uint8Array {
	const length = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0)
	const out: Uint8Array = new Uint8Array(length)
	let offset = 0
	for (const chunk of chunks) {
		out.set(chunk, offset)
		offset += chunk.byteLength
	}
	return out
}

function toUint8Array(chunk: Uint8Array): Uint8Array {
	return copyBytes(chunk)
}

async function messageDataToBytes(data: unknown): Promise<Uint8Array> {
	if (typeof data === "string") throw new Error("QMux WebSocket received text data")
	if (data instanceof ArrayBuffer) return new Uint8Array(data.slice(0))
	if (ArrayBuffer.isView(data)) return copyBytes(new Uint8Array(data.buffer, data.byteOffset, data.byteLength))
	if (typeof Blob !== "undefined" && data instanceof Blob) return new Uint8Array(await data.arrayBuffer())
	throw new Error("QMux WebSocket received unsupported message data")
}

function copyBytes(bytes: Uint8Array): Uint8Array {
	const copy: Uint8Array = new Uint8Array(bytes.byteLength)
	copy.set(bytes)
	return copy
}

function isUnidirectional(streamId: number): boolean {
	return (streamId & 0x02) !== 0
}

function webSocketReason(reason: string): string {
	return reason.length > 123 ? reason.slice(0, 123) : reason
}
