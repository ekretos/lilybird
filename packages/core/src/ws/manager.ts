import { DebugIdentifier, GatewayOpCode } from "#enums";

import type {
    UpdatePresenceStructure,
    GetGatewayBotResponse,
    ReceiveDispatchEvent,
    UpdatePresence,
    DebugFunction,
    Identify,
    Payload,
    Resume
} from "../typings/index.js";

interface ManagerOptions {
    token?: string;
    intents: number;
    presence?: UpdatePresenceStructure;
    reconnect?: boolean;
    maxReconnectAttempts?: number;
    reconnectBaseDelay?: number;
    reconnectMaxDelay?: number;
}

export type DispatchFunction = (data: ReceiveDispatchEvent) => any;

type Socket = WebSocket & {
    ping?: () => void;
};

export class WebSocketManager {
    readonly #dispatch: DispatchFunction;
    readonly #debug: DebugFunction | undefined;

    #sequenceNumber: number | null = null;
    #isResuming = false;
    #ws?: Socket;
    #gatewayInfo?: GetGatewayBotResponse;
    #options: Required<ManagerOptions>;
    #timer?: ReturnType<typeof setInterval>;
    #reconnectTimer?: ReturnType<typeof setTimeout>;
    #reconnectAttempts = 0;
    #generation = 0;
    #closed = false;
    #gotACK = true;
    #heartbeatInterval = 0;
    #heartbeatPending = false;

    public readonly resumeInfo: {
        url: string,
        id: string
    } = <never>{};

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
        await this.#connect(url ?? this.#getGatewayUrl());
    }

    async #connect(url: string): Promise<void> {
        const generation = ++this.#generation;
        this.#clearTimer();
        this.#heartbeatPending = false;

        const ws = <Socket>new WebSocket(url);
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

            if (code === 4004) {
                this.#isResuming = false;
                this.#debug?.(DebugIdentifier.InvalidSession);
                return;
            }

            if (code === 4010 || code === 4011 || code === 4012 || code === 4013 || code === 4014) {
                this.#isResuming = false;
                return;
            }

            if (code === 4007 || code === 4009) {
                this.#isResuming = false;
                this.#sequenceNumber = null;
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

            switch (payload.op) {
                case GatewayOpCode.Dispatch:
                    this.#dispatch(payload);
                    break;
                case GatewayOpCode.Hello: {
                    const interval = payload.d.heartbeat_interval;
                    this.#startTimer(interval);
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
                    if (payload.d === true && this.#canResume()) {
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
                    this.#gotACK = true;
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
            const timeout = setTimeout(() => controller.abort(), 15_000);
            let response: Response;
            try {
                response = await fetch("https://discord.com/api/v10/gateway/bot", {
                    headers: {
                        Authorization: `Bot ${this.#options.token}`
                    },
                    signal: controller.signal
                });
            } finally {
                clearTimeout(timeout);
            }

            if (!response.ok) throw new Error("An invalid Token was provided");
            const data: GetGatewayBotResponse = await response.json() as never;
            data.url = `${data.url}/?v=10&encoding=json`;
            this.#gatewayInfo = data;
        }

        return this.#gatewayInfo.url;
    }

    #canResume(): boolean {
        return typeof this.resumeInfo.id === "string" && this.resumeInfo.id.length > 0 && this.#sequenceNumber !== null;
    }

    #scheduleReconnect(resume: boolean): void {
        if (!this.#options.reconnect || this.#closed || this.#reconnectTimer !== undefined) return;
        if (this.#reconnectAttempts >= this.#options.maxReconnectAttempts) return;

        this.#isResuming = resume && this.#canResume();
        const attempt = this.#reconnectAttempts++;
        const exponential = Math.min(this.#options.reconnectBaseDelay * 2 ** attempt, this.#options.reconnectMaxDelay);
        const delay = exponential + Math.floor(Math.random() * Math.min(1_000, exponential * 0.25));

        this.#reconnectTimer = setTimeout(async () => {
            this.#reconnectTimer = undefined;
            if (this.#closed) return;
            try {
                const url = this.#isResuming && this.resumeInfo.url.length > 0
                    ? `${this.resumeInfo.url}/?v=10&encoding=json`
                    : await this.#getGatewayUrl();
                await this.#connect(url);
            } catch {
                this.#scheduleReconnect(this.#isResuming);
            }
        }, delay);
    }

    #getInterval(interval: number): number {
        return Math.floor(Math.random() * interval);
    }

    #sendHeartbeatPayload(): void {
        if (typeof this.#ws === "undefined" || this.#ws.readyState !== WebSocket.OPEN) return;
        this.#gotACK = false;
        this.#heartbeatPending = true;
        this.#ws.send(JSON.stringify({
            op: GatewayOpCode.Heartbeat,
            d: this.#sequenceNumber,
            s: null,
            t: null
        }));
    }

    #identify(): void {
        if (typeof this.#options.token === "undefined") throw new Error("No token was found");

        const payload: Identify = {
            op: GatewayOpCode.Identify,
            d: {
                token: this.#options.token,
                intents: this.#options.intents,
                properties: {
                    os: process.platform,
                    browser: "Lilybird",
                    device: "Lilybird"
                },
                presence: this.#options.presence
            },
            s: null,
            t: null
        };

        this.#debug?.(DebugIdentifier.Identify);
        this.#ws?.send(JSON.stringify(payload));
    }

    #resume(): void {
        if (!this.#canResume()) {
            this.#isResuming = false;
            this.#identify();
            return;
        }

        const payload: Resume = {
            op: GatewayOpCode.Resume,
            d: {
                token: this.#options.token,
                session_id: this.resumeInfo.id,
                seq: this.#sequenceNumber as number
            },
            s: null,
            t: null
        };

        this.#debug?.(DebugIdentifier.Resume);
        this.#ws?.send(JSON.stringify(payload));
    }

    #startTimer(interval: number): void {
        this.#clearTimer();
        this.#heartbeatInterval = interval;
        this.#gotACK = true;
        this.#heartbeatPending = false;
        this.#timer = setInterval(() => {
            if (this.#heartbeatPending && !this.#gotACK) {
                this.#debug?.(DebugIdentifier.MissingACK);
                this.#debug?.(DebugIdentifier.ZombieConnection);
                this.#ws?.close(1001);
                return;
            }

            this.#debug?.(DebugIdentifier.Heartbeat);
            this.#sendHeartbeatPayload();
        }, this.#getInterval(interval));
    }

    #clearTimer(): void {
        if (typeof this.#timer === "undefined") return;
        clearInterval(this.#timer);
        this.#timer = undefined;
    }

    #clearReconnectTimer(): void {
        if (typeof this.#reconnectTimer === "undefined") return;
        clearTimeout(this.#reconnectTimer);
        this.#reconnectTimer = undefined;
    }

    #clearResumeInfo(): void {
        this.resumeInfo.url = "";
        this.resumeInfo.id = "";
    }

    public async ping(): Promise<number> {
        if (typeof this.#ws === "undefined" || typeof this.#ws.ping !== "function") throw new Error("WebSocket is not connected");

        return new Promise((resolve, reject) => {
            const start = performance.now();
            const timeout = setTimeout(() => reject(new Error("WebSocket ping timed out")), 5_000);
            this.#ws?.addEventListener("pong", () => {
                clearTimeout(timeout);
                resolve(performance.now() - start);
            }, { once: true });
            this.#ws?.ping?.();
        });
    }

    public updatePresence(presence: UpdatePresenceStructure): void {
        if (typeof this.#ws === "undefined" || this.#ws.readyState !== WebSocket.OPEN) throw new Error("WebSocket is not connected");

        const options: UpdatePresence = {
            op: GatewayOpCode.PresenceUpdate,
            d: presence,
            s: null,
            t: null
        };

        this.#ws.send(JSON.stringify(options));
    }

    public set options(options: Partial<ManagerOptions>) {
        this.#options = { ...this.#options, ...options };
    }

    public get options(): ManagerOptions {
        return this.#options;
    }
}
