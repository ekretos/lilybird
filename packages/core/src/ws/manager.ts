import { DebugIdentifier, GatewayOpCode } from "#enums";

import type { UpdatePresenceStructure, GetGatewayBotResponse, ReceiveDispatchEvent, UpdatePresence, DebugFunction, Identify, Payload, Resume } from "../typings/index.js";

interface ManagerOptions {
    token?: string;
    intents: number;
    presence?: UpdatePresenceStructure;
    reconnect?: boolean;
    maxReconnectAttempts?: number;
    reconnectBaseDelay?: number;
    reconnectMaxDelay?: number;
    shard?: [number, number];
}

export type DispatchFunction = (data: ReceiveDispatchEvent) => any;
type Socket = WebSocket & { ping?: () => void };

export class WebSocketManager {
    readonly #dispatch: DispatchFunction;
    readonly #debug: DebugFunction | undefined;
    #sequenceNumber: number | null = null;
    #isResuming = false;
    #ws?: Socket;
    #gatewayInfo?: GetGatewayBotResponse;
    #options: ManagerOptions & Required<Pick<ManagerOptions, "reconnect" | "maxReconnectAttempts" | "reconnectBaseDelay" | "reconnectMaxDelay">>;
    #timer?: ReturnType<typeof setInterval>;
    #reconnectTimer?: ReturnType<typeof setTimeout>;
    #reconnectAttempts = 0;
    #generation = 0;
    #closed = false;
    #gotAck = true;
    #heartbeatPending = false;

    public readonly resumeInfo: { url: string, id: string } = <never>{};

    public constructor(options: ManagerOptions, dispatch: DispatchFunction, debug?: DebugFunction) {
        if (typeof options.intents !== "number" || Number.isNaN(options.intents)) throw new Error("Invalid intents");
        this.#dispatch = dispatch;
        this.#debug = debug;
        this.#options = {
            reconnect: true,
            maxReconnectAttempts: Infinity,
            reconnectBaseDelay: 1_000,
            reconnectMaxDelay: 30_000,
            ...options
        };
    }

    public close(): void {
        this.#closed = true;
        this.#clearReconnectTimer();
        this.#clearTimer();
        this.#ws?.close(3000);
        this.#ws = undefined;
        this.#isResuming = false;
        this.#heartbeatPending = false;
    }

    public async connect(url?: string): Promise<void> {
        this.#closed = false;
        this.#clearReconnectTimer();
        this.#connect(url ?? await this.#getGatewayUrl());
    }

    #connect(url: string): void {
        const generation = ++this.#generation;
        this.#clearTimer();
        this.#heartbeatPending = false;
        const ws = <Socket> new WebSocket(url);
        this.#ws = ws;

        ws.addEventListener("open", () => {
            if (generation !== this.#generation) return;
            this.#reconnectAttempts = 0;
        });
        ws.addEventListener("error", (err) => {
            if (generation !== this.#generation) return;
            this.#debug?.(DebugIdentifier.WSError, err);
        });
        ws.addEventListener("close", ({ code }) => {
            if (generation !== this.#generation) return;
            this.#debug?.(DebugIdentifier.CloseCode, code);
            this.#clearTimer();
            this.#heartbeatPending = false;
            if (this.#closed || code === 3000) return;
            if (code === 4004 || code === 4010 || code === 4011 || code === 4012 || code === 4013 || code === 4014) return;
            if (code === 4007 || code === 4009) {
                this.#isResuming = false;
                this.#sequenceNumber = null;
                this.#clearResumeInfo();
            }
            if (code >= 4000 && code < 5000 && code !== 4007 && code !== 4008 && code !== 4009) {
                this.#scheduleReconnect(false);
                return;
            }
            this.#scheduleReconnect(this.#canResume());
        });
        ws.addEventListener("message", (event) => {
            if (generation !== this.#generation) return;
            this.#debug?.(DebugIdentifier.WSMessage, event.data);
            let payload: Payload;
            try {
                payload = <Payload>JSON.parse(String(event.data));
            } catch {
                ws.close(1002);
                return;
            }
            if (typeof payload.s === "number") this.#sequenceNumber = payload.s;

            // eslint-disable-next-line @typescript-eslint/switch-exhaustiveness-check
            switch (payload.op) {
                case GatewayOpCode.Dispatch:
                    this.#dispatch(payload);
                    break;
                case GatewayOpCode.Hello: {
                    this.#startTimer(payload.d.heartbeat_interval);
                    if (this.#isResuming && this.#canResume()) this.#resume();
                    else {
                        this.#isResuming = false;
                        this.#identify();
                    }
                    break;
                }
                case GatewayOpCode.Heartbeat:
                    this.#sendHeartbeatPayload();
                    break;
                case GatewayOpCode.Reconnect:
                    ws.close(1001);
                    break;
                case GatewayOpCode.InvalidSession:
                    this.#debug?.(DebugIdentifier.InvalidSession);
                    if (payload.d && this.#canResume()) {
                        this.#isResuming = true;
                        ws.close(1001);
                    } else {
                        this.#isResuming = false;
                        this.#sequenceNumber = null;
                        this.#clearResumeInfo();
                        ws.close(1000);
                    }
                    break;
                case GatewayOpCode.HeartbeatACK:
                    this.#gotAck = true;
                    this.#heartbeatPending = false;
                    this.#debug?.(DebugIdentifier.ACK);
                    break;
                default:
                    break;
            }
        });
    }

    async #getGatewayUrl(): Promise<string> {
        if (typeof this.#gatewayInfo === "undefined") {
            const controller = new AbortController();
            const timeout = setTimeout(() => { controller.abort(); }, 15_000);
            let response: Response;
            try {
                response = await fetch("https://discord.com/api/v10/gateway/bot", { headers: { Authorization: `Bot ${this.#options.token}` }, signal: controller.signal });
            } finally { clearTimeout(timeout); }
            if (!response.ok) throw new Error("An invalid Token was provided");
            const data: GetGatewayBotResponse = await response.json() as never;
            data.url = `${data.url}/?v=10&encoding=json`;
            this.#gatewayInfo = data;
        }
        return this.#gatewayInfo.url;
    }

    #canResume(): boolean { return typeof this.resumeInfo.id === "string" && this.resumeInfo.id.length > 0 && this.#sequenceNumber !== null; }
    #scheduleReconnect(resume: boolean): void {
        if (!this.#options.reconnect || this.#closed || this.#reconnectTimer !== undefined || this.#reconnectAttempts >= this.#options.maxReconnectAttempts) return;
        this.#isResuming = resume && this.#canResume();
        const attempt = this.#reconnectAttempts++;
        const exponential = Math.min(this.#options.reconnectBaseDelay * Math.pow(2, attempt), this.#options.reconnectMaxDelay);
        const delay = exponential + Math.floor(Math.random() * Math.min(1_000, exponential * 0.25));
        this.#reconnectTimer = setTimeout(async () => {
            this.#reconnectTimer = undefined;
            if (this.#closed) return;
            try {
                const url = this.#isResuming && this.resumeInfo.url.length > 0 ? `${this.resumeInfo.url}/?v=10&encoding=json` : await this.#getGatewayUrl();
                this.#connect(url);
            } catch { this.#scheduleReconnect(this.#isResuming); }
        }, delay);
    }

    #sendHeartbeatPayload(): void {
        if (typeof this.#ws === "undefined" || this.#ws.readyState !== WebSocket.OPEN) return;
        this.#gotAck = false;
        this.#heartbeatPending = true;
        this.#ws.send(JSON.stringify({ op: GatewayOpCode.Heartbeat, d: this.#sequenceNumber, s: null, t: null }));
    }

    #identify(): void {
        if (typeof this.#options.token === "undefined") throw new Error("No token was found");
        const payload: Identify = {
            op: GatewayOpCode.Identify,
            d: {
                token: this.#options.token,
                intents: this.#options.intents,
                properties: { os: process.platform, browser: "Lilybird", device: "Lilybird" },
                presence: this.#options.presence,
                shard: this.#options.shard
            },
            s: null,
            t: null
        };
        this.#debug?.(DebugIdentifier.Identify);
        this.#ws?.send(JSON.stringify(payload));
    }

    #resume(): void {
        if (!this.#canResume() || this.#sequenceNumber === null) {
            this.#isResuming = false;
            this.#identify();
            return;
        }
        const { token } = this.#options;
        if (typeof token === "undefined") throw new Error("No token was found");
        const payload: Resume = { op: GatewayOpCode.Resume, d: { token, session_id: this.resumeInfo.id, seq: this.#sequenceNumber }, s: null, t: null };
        this.#debug?.(DebugIdentifier.Resume);
        this.#ws?.send(JSON.stringify(payload));
    }

    #startTimer(interval: number): void {
        this.#clearTimer();
        this.#gotAck = true;
        this.#heartbeatPending = false;
        this.#timer = setInterval(() => {
            if (this.#heartbeatPending && !this.#gotAck) {
                this.#debug?.(DebugIdentifier.MissingACK);
                this.#debug?.(DebugIdentifier.ZombieConnection);
                this.#ws?.close(1001);
                return;
            }
            this.#debug?.(DebugIdentifier.Heartbeat);
            this.#sendHeartbeatPayload();
        }, Math.max(1, Math.floor(Math.random() * interval)));
    }

    #clearTimer(): void {
        if (this.#timer !== undefined) {
            clearInterval(this.#timer);
            this.#timer = undefined;
        }
    }

    #clearReconnectTimer(): void {
        if (this.#reconnectTimer !== undefined) {
            clearTimeout(this.#reconnectTimer);
            this.#reconnectTimer = undefined;
        }
    }

    #clearResumeInfo(): void {
        this.resumeInfo.url = "";
        this.resumeInfo.id = "";
    }

    public async ping(): Promise<number> {
        if (typeof this.#ws === "undefined" || typeof this.#ws.ping !== "function") throw new Error("WebSocket is not connected");
        return new Promise((resolve, reject) => {
            const start = performance.now();
            const timeout = setTimeout(() => { reject(new Error("WebSocket ping timed out")); }, 5_000);
            this.#ws?.addEventListener("pong", () => {
                clearTimeout(timeout);
                resolve(performance.now() - start);
            }, { once: true });
            this.#ws?.ping();
        });
    }

    public updatePresence(presence: UpdatePresenceStructure): void {
        if (typeof this.#ws === "undefined" || this.#ws.readyState !== WebSocket.OPEN) throw new Error("WebSocket is not connected");
        const options: UpdatePresence = { op: GatewayOpCode.PresenceUpdate, d: presence, s: null, t: null };
        this.#ws.send(JSON.stringify(options));
    }

    public set options(options: Partial<ManagerOptions>) { this.#options = { ...this.#options, ...options }; }
    public get options(): ManagerOptions { return this.#options; }
}

export interface ShardManagerOptions {
    token: string;
    intents: number;
    presence?: UpdatePresenceStructure;
    shardCount?: number;
    maxConcurrency?: number;
    reconnect?: boolean;
}

export interface ShardDispatch {
    shardId: number;
    data: ReceiveDispatchEvent;
}

export class ShardManager {
    readonly #options: ShardManagerOptions;
    readonly #dispatch: (event: ShardDispatch) => any;
    readonly #debug?: DebugFunction;
    readonly #shards = new Map<number, WebSocketManager>();
    #gateway?: GetGatewayBotResponse;

    public constructor(options: ShardManagerOptions, dispatch: (event: ShardDispatch) => any, debug?: DebugFunction) {
        if (!options.token) throw new Error("No token was found");
        if (!Number.isInteger(options.intents) || options.intents < 0) throw new Error("Invalid intents");
        this.#options = options;
        this.#dispatch = dispatch;
        this.#debug = debug;
    }

    public get size(): number { return this.#shards.size; }
    public get shards(): ReadonlyMap<number, WebSocketManager> { return this.#shards; }
    public get gatewayInfo(): GetGatewayBotResponse | undefined { return this.#gateway; }

    public async connect(): Promise<void> {
        this.#gateway = await this.#getGatewayBot();
        const count = this.#options.shardCount ?? this.#gateway.shards;
        if (!Number.isInteger(count) || count < 1 || count > this.#gateway.shards) throw new Error(`Invalid shard count: ${count}`);
        const concurrency = Math.max(1, Math.min(this.#options.maxConcurrency ?? this.#gateway.session_start_limit.max_concurrency, this.#gateway.session_start_limit.max_concurrency));
        const url = `${this.#gateway.url}/?v=10&encoding=json`;

        for (let start = 0; start < count; start += concurrency) {
            const batch: Array<Promise<void>> = [];
            for (let id = start; id < Math.min(start + concurrency, count); id++) batch.push(this.#connectShard(id, count, url));
            // eslint-disable-next-line no-await-in-loop
            await Promise.all(batch);
            // eslint-disable-next-line no-await-in-loop
            if (start + concurrency < count) await new Promise((resolve) => { setTimeout(resolve, 5_000); });
        }
    }

    async #connectShard(id: number, count: number, url: string): Promise<void> {
        const manager = new WebSocketManager({
            token: this.#options.token,
            intents: this.#options.intents,
            presence: this.#options.presence,
            reconnect: this.#options.reconnect,
            shard: [id, count]
        }, (data) => {
            this.#dispatch({ shardId: id, data });
        }, this.#debug);
        this.#shards.set(id, manager);
        await manager.connect(url);
    }

    public close(): void { for (const shard of this.#shards.values()) shard.close(); }
    public setPresence(presence: UpdatePresenceStructure): void { for (const shard of this.#shards.values()) shard.updatePresence(presence); }

    async #getGatewayBot(): Promise<GetGatewayBotResponse> {
        const response = await fetch("https://discord.com/api/v10/gateway/bot", { headers: { Authorization: `Bot ${this.#options.token}` } });
        if (!response.ok) throw new Error("Unable to retrieve gateway information");
        return await response.json() as GetGatewayBotResponse;
    }
}
