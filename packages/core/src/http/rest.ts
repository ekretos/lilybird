//@ts-expect-error We don't want the package.json being added to dist
import packageJson from "../../package.json" with { type: "json" };
import { DebugIdentifier } from "#enums";

import type { AuditLogEvent, MFALevel, OnboardingMode, PrivacyLevel } from "#enums";
import type {
    ListArchivedThreadsReturnStructure, GetGatewayBotResponse, ApplicationCommand, LilybirdAttachment, AutoModeration, DebugFunction,
    StageInstance, Application, Interaction, AuditLog, Channel, Message, Sticker, Webhook, Invite, Emoji, Guild, Voice, Role, User, Poll
} from "../typings/index.js";

export interface DiscordErrorMessage { code: number; message: string; errors?: Record<string, unknown>; }

export class RestError extends Error {
    public readonly code: number; public readonly errors: DiscordErrorMessage["errors"]; public readonly status: number;
    public constructor(error: DiscordErrorMessage, status: number) { super(error.message); this.name = "RestError"; this.code = error.code; this.errors = error.errors; this.status = status; }
}
export class RestRateLimitError extends RestError {
    public readonly retryAfter: number; public readonly global: boolean;
    public constructor(error: DiscordErrorMessage, status: number, retryAfter: number, global: boolean) { super(error, status); this.name = "RestRateLimitError"; this.retryAfter = retryAfter; this.global = global; }
}
export class RestNetworkError extends Error {
    public readonly cause: unknown;
    public constructor(cause: unknown) { super(cause instanceof Error ? cause.message : "REST request failed"); this.name = "RestNetworkError"; this.cause = cause; }
}

// I ran out of ideas for naming this thing
type ExtractedData = ({ data: { attachments: Array<unknown> | undefined } } | { attachments: Array<unknown> | undefined }) & { reason?: string };
type RequestMethod = "GET" | "POST" | "PATCH" | "DELETE" | "PUT";
interface RESTOptions { timeout?: number; maxRetries?: number; }

export class REST {
    public static readonly BaseURL = "https://discord.com/api/v10/"; public static readonly DefaultTimeout = 15_000; public static readonly DefaultMaxRetries = 3;
    #token?: string; readonly #timeout: number; readonly #maxRetries: number;
    public constructor(token?: string, options: RESTOptions = {}) { this.#token = token; this.#timeout = options.timeout ?? REST.DefaultTimeout; this.#maxRetries = Math.max(0, options.maxRetries ?? REST.DefaultMaxRetries); }
    public async makeAPIRequest<T>(method: RequestMethod, path: string, data: FormData, reason?: string): Promise<T>;
    public async makeAPIRequest<T>(method: RequestMethod, path: string, data?: Record<string, any>, files?: Array<LilybirdAttachment>): Promise<T>;
    public async makeAPIRequest<T>(method: RequestMethod, path: string, data?: Record<string, any> | FormData, filesOrReason?: string | Array<LilybirdAttachment>): Promise<T> {
        const opts: RequestInit = { method, headers: { Authorization: `Bot ${this.#token}`, "User-Agent": `DiscordBot/LilyBird/${(<{ version: string }>packageJson).version}` } };
        if (data instanceof FormData) { opts.body = data; if (typeof filesOrReason !== "undefined") { // @ts-expect-error No comments
                opts.headers["X-Audit-Log-Reason"] = filesOrReason; }
        } else if (typeof data !== "undefined") {
            let reason: string | undefined; let obj: ExtractedData;
            if ("reason" in data) ({ reason, ...obj } = data as ExtractedData); else obj = data as never;
            if (typeof filesOrReason !== "undefined" && typeof filesOrReason !== "string" && filesOrReason.length > 0) {
                const temp: Array<Partial<Channel.AttachmentStructure>> = []; const form = new FormData();
                for (let i = 0, { length } = filesOrReason; i < length; i++) { form.append(`files[${i}]`, filesOrReason[i].file, filesOrReason[i].name); temp.push({ id: i, filename: filesOrReason[i].name }); }
                if ("data" in obj) obj.data.attachments = [...temp, ...obj.data.attachments ?? []]; else obj.attachments = [...temp, ...obj.attachments ?? []];
                form.append("payload_json", JSON.stringify(obj)); opts.body = form;
            } else { // @ts-expect-error No comments
                opts.headers["Content-Type"] = "application/json"; opts.body = JSON.stringify(data); }
            if (typeof reason !== "undefined") { // @ts-expect-error No comments
                opts.headers["X-Audit-Log-Reason"] = reason; }
        }
        return this.#request<T>(method, path, opts);
    }
    public setToken(token: string | undefined): void { this.#token = token; }
    #isRetryable(method: RequestMethod, status: number): boolean { if (method === "GET" || method === "PUT" || method === "DELETE") return status === 429 || status >= 500; return false; }
    #backoff(attempt: number): number { return Math.min(1_000 * 2 ** attempt, 10_000) + Math.floor(Math.random() * 250); }
    async #request<T>(method: RequestMethod, path: string, opts: RequestInit): Promise<T> {
        for (let attempt = 0; attempt <= this.#maxRetries; attempt++) {
            const controller = new AbortController(); const timeout = setTimeout(() => controller.abort(), this.#timeout); opts.signal = controller.signal; let response: Response;
            try { response = await fetch(`${REST.BaseURL}${path}`, opts); } catch (error) {
                if (attempt < this.#maxRetries && method === "GET") { clearTimeout(timeout); await new Promise((resolve) => setTimeout(resolve, this.#backoff(attempt))); continue; }
                clearTimeout(timeout); throw new RestNetworkError(error);
            }
            clearTimeout(timeout);
            if (response.ok) { if (response.status === 204) return <T>null; return <T>await response.json(); }
            let errorMessage: DiscordErrorMessage; try { errorMessage = await response.json() as DiscordErrorMessage; } catch { errorMessage = { code: response.status, message: response.statusText || "Discord API request failed" }; }
            if (response.status === 429) {
                const retryAfterHeader = response.headers.get("Retry-After"); const retryAfterBody = typeof (errorMessage as DiscordErrorMessage & { retry_after?: number }).retry_after === "number" ? (errorMessage as DiscordErrorMessage & { retry_after: number }).retry_after : undefined;
                const retryAfter = retryAfterBody ?? (retryAfterHeader ? Number(retryAfterHeader) * 1_000 : 1_000); const global = (errorMessage as DiscordErrorMessage & { global?: boolean }).global === true;
                if (attempt < this.#maxRetries) { await new Promise((resolve) => setTimeout(resolve, Math.max(0, retryAfter))); continue; }
                throw new RestRateLimitError(errorMessage, response.status, retryAfter, global);
            }
            if (response.status >= 500 && attempt < this.#maxRetries && this.#isRetryable(method, response.status)) { await new Promise((resolve) => setTimeout(resolve, this.#backoff(attempt))); continue; }
            throw new RestError(errorMessage, response.status);
        }
        throw new Error("REST request retry limit exceeded");
    }
    public async getGateway(): Promise<{ url: string }> { return this.makeAPIRequest("GET", "gateway"); }
    public async getGatewayBot(): Promise<GetGatewayBotResponse> { return this.makeAPIRequest("GET", "gateway/bot"); }
    public async getGlobalApplicationCommands(clientId: string): Promise<Array<ApplicationCommand.GlobalStructure>> { return this.makeAPIRequest("GET", `applications/${clientId}/commands?with_localizations=false`); }
    public async getGlobalApplicationCommand(clientId: string, commandId: string): Promise<ApplicationCommand.Localizations.GlobalStructure> { return this.makeAPIRequest("GET", `applications/${clientId}/commands/${commandId}`); }
    public async createInteractionResponse(interactionId: string, interactionToken: string, body: Interaction.ResponseJSONParams, files?: Array<LilybirdAttachment>): Promise<null> { return this.makeAPIRequest("POST", `interactions/${interactionId}/${interactionToken}/callback`, body, files); }
}

export class DebugREST extends REST { public override async makeAPIRequest<T>(method: RequestMethod, path: string, data?: Record<string, any> | FormData, filesOrReason?: string | Array<LilybirdAttachment>): Promise<T> { return super.makeAPIRequest<T>(method, path, data as never, filesOrReason as never); } }
