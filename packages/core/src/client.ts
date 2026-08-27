import { CachingManager } from "./cache/manager.js";
import { DebugREST, REST } from "./http/rest.js";
import { ListenerCompiler } from "./compiler.js";
import { WebSocketManager } from "#ws";

import type { CompilerOptions } from "./compiler.js";
import type { DispatchFunction } from "#ws";
import type {
    CacheManagerStructure,
    CachingOptions,
    ClientOptions,
    DebugFunction,
    Transformers,
    Application,
    MockClient,
    Listeners
} from "./typings/index.js";

type GetUserType<T extends Transformers<any>> = (T["userUpdate"] & {}) extends { handler: ((...args: infer U) => infer R) }
    ? unknown extends R ? U[1] : R
    : never;

export class Client implements MockClient {
    public readonly rest: REST;
    public readonly cache: CacheManagerStructure;
    public readonly ws: WebSocketManager;
    public readonly declare user: GetUserType<Transformers<this>>;
    public readonly declare sessionId: string;
    public readonly declare application: Application.Structure;
    protected readonly declare ready: boolean;
    #dispatch?: DispatchFunction;

    public constructor(options: ClientOptions, debug?: DebugFunction) {
        this.rest = options.useDebugRest === true ? new DebugREST(debug) : new REST();
        this.cache = typeof options.cachingManager !== "undefined" ? options.cachingManager : new CachingManager();
        this.#dispatch = options.dispatch;
        this.ws = new WebSocketManager(
            { intents: options.intents, presence: options.presence },
            (payload) => this.#dispatch?.(payload),
            debug
        );
    }

    public async login(token: string, dispatch: DispatchFunction | undefined = this.#dispatch): Promise<string> {
        if (typeof dispatch === "undefined") throw new Error("the client doesn't have any 'dispatch' function defined.");
        this.#dispatch = dispatch;
        this.ws.options = { token };
        this.rest.setToken(token);
        await this.ws.connect();
        return token;
    }

    public close(): void {
        this.rest.setToken(undefined);
        this.ws.close();
    }

    public async ping(): Promise<{ ws: number, rest: number }> {
        const start = performance.now();
        await this.rest.getGateway();
        return { ws: await this.ws.ping(), rest: performance.now() - start };
    }
}

export interface CreateClientOptions<T extends Transformers<any>> extends Omit<ClientOptions, "dispatch">, CompilerOptions<T> {
    token: string;
    listeners: Listeners<Client, T>;
    caching?: CachingOptions;
    debug?: DebugFunction;
}

export async function createClient<T extends Transformers<Client> = Transformers<Client>>(options: CreateClientOptions<T>): Promise<Client> {
    const compiler = new ListenerCompiler<Client, T>({ transformers: options.transformers, transformClient: options.transformClient });
    compiler.addListenersFromObject(options.listeners);
    if (typeof options.caching !== "undefined") compiler.appendCachingHandlers(options.caching);

    const client = new Client({
        intents: options.intents,
        presence: options.presence,
        useDebugRest: typeof options.debug !== "undefined",
        cachingManager: options.cachingManager
    }, options.debug);

    await client.login(options.token, compiler.getDispatchFunction(client, client.ws.resumeInfo));
    return client;
}
