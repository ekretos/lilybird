import { describe, expect, it, mock, beforeEach, afterEach } from "bun:test";
import { REST, RestError, RestNetworkError, RestRateLimitError } from "./rest.js";

describe("REST reliability", () => {
    const originalFetch = globalThis.fetch;

    beforeEach(() => {
        mock.restore();
    });

    afterEach(() => {
        globalThis.fetch = originalFetch;
    });

    it("returns successful responses", async () => {
        globalThis.fetch = mock(async () => new Response(JSON.stringify({ id: "1" }), { status: 200 }));
        const rest = new REST("token", { maxRetries: 0 });
        await expect(rest.makeAPIRequest("GET", "users/@me")).resolves.toEqual({ id: "1" });
    });

    it("retries 429 responses using Retry-After", async () => {
        let calls = 0;
        globalThis.fetch = mock(async () => {
            calls++;
            if (calls === 1) return new Response(JSON.stringify({ code: 20028, message: "rate limited", retry_after: 0.001, global: false }), { status: 429, headers: { "Retry-After": "0.001" } });
            return new Response(JSON.stringify({ ok: true }), { status: 200 });
        });

        const rest = new REST("token", { maxRetries: 1 });
        await expect(rest.makeAPIRequest("GET", "users/@me")).resolves.toEqual({ ok: true });
        expect(calls).toBe(2);
    });

    it("throws a typed rate limit error after retries", async () => {
        globalThis.fetch = mock(async () => new Response(JSON.stringify({ code: 20028, message: "rate limited", retry_after: 0, global: true }), { status: 429 }));
        const rest = new REST("token", { maxRetries: 0 });
        const error = await rest.makeAPIRequest("GET", "users/@me").catch((value) => value);
        expect(error).toBeInstanceOf(RestRateLimitError);
        expect(error.global).toBe(true);
    });

    it("retries idempotent 5xx responses", async () => {
        let calls = 0;
        globalThis.fetch = mock(async () => {
            calls++;
            return calls === 1
                ? new Response("", { status: 503 })
                : new Response(JSON.stringify({ ok: true }), { status: 200 });
        });

        const rest = new REST("token", { maxRetries: 1 });
        await expect(rest.makeAPIRequest("GET", "users/@me")).resolves.toEqual({ ok: true });
        expect(calls).toBe(2);
    });

    it("does not retry non-idempotent 5xx responses", async () => {
        let calls = 0;
        globalThis.fetch = mock(async () => {
            calls++;
            return new Response("", { status: 503 });
        });

        const rest = new REST("token", { maxRetries: 3 });
        const error = await rest.makeAPIRequest("POST", "channels/1/messages", { content: "test" }).catch((value) => value);
        expect(error).toBeInstanceOf(RestError);
        expect(calls).toBe(1);
    });

    it("wraps network failures", async () => {
        globalThis.fetch = mock(async () => {
            throw new Error("socket failure");
        });

        const rest = new REST("token", { maxRetries: 0 });
        const error = await rest.makeAPIRequest("GET", "users/@me").catch((value) => value);
        expect(error).toBeInstanceOf(RestNetworkError);
    });
});
