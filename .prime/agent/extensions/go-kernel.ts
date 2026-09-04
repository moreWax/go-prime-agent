/**
 * go-kernel extension — runs Go cells in a persistent gorlm kernel.
 *
 * Ships the go-prime-agent runtime as a Prime Agent extension:
 *   - registers a `go` tool that owns a gorlm process (RLM protocol v3:
 *     NDJSON over stdio) with persistent state across cells
 *   - blocks the built-in ipython tool so the Python kernel never boots
 *   - services gorlm host_request frames; unbridged kinds get honest
 *     error replies (cells can handle them in Go)
 *
 * Kernel binary resolution: $GORLM_BIN, then <cwd>/bin/gorlm, then
 * /tmp/gorlm.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { defineTool, type ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { randomUUID } from "node:crypto";

interface PendingRequest {
	output: string[];
	result?: string;
	error?: { ename: string; evalue: string };
	resolve: () => void;
}

const PROTOCOL_TIMEOUT_MS = 30_000;
const CELL_TIMEOUT_MS = 120_000;

/**
 * Kernel binary: $GORLM_BIN, else <repo>/bin/gorlm located relative to this
 * extension file (the extension lives at <repo>/.prime/agent/extensions/).
 */
function kernelBinary(): string {
	const fromEnv = process.env.GORLM_BIN;
	if (fromEnv) return fromEnv;
	const here = new URL(".", import.meta.url).pathname;
	const local = resolve(here, "../../../bin/gorlm");
	if (existsSync(local)) return local;
	throw new Error(
		`gorlm not found at ${local} — run \`make build\` in the repo or set GORLM_BIN`,
	);
}

class GoKernel {
	private proc: ChildProcess | null = null;
	private pending = new Map<string, PendingRequest>();
	private ready: Promise<void> | null = null;
	private writer: ((line: string) => void) | null = null;
	private exitInfo: string | null = null;

	constructor(private bin: string) {}

	start(): Promise<void> {
		if (this.ready) return this.ready;
		this.ready = new Promise<void>((resolveStart, rejectStart) => {
			let settled = false;
			const proc = spawn(this.bin, [], { stdio: ["pipe", "pipe", "pipe"] });
			this.proc = proc;
			let buffer = "";
			this.writer = (line) => proc.stdin?.write(line + "\n");

			proc.stdout!.setEncoding("utf8");
			proc.stdout!.on("data", (chunk: string) => {
				buffer += chunk;
				let nl: number;
				while ((nl = buffer.indexOf("\n")) >= 0) {
					const line = buffer.slice(0, nl);
					buffer = buffer.slice(nl + 1);
					if (line.trim()) this.onEvent(line, settled ? undefined : resolveStart);
				}
			});
			proc.stderr!.setEncoding("utf8");
			proc.stderr!.on("data", (chunk: string) => {
				process.stderr.write(`[gorlm] ${chunk}`);
			});
			proc.on("exit", (code, signal) => {
				this.exitInfo = `kernel exited (code=${code} signal=${signal})`;
				for (const p of this.pending.values()) p.resolve();
				this.pending.clear();
				if (!settled) {
					settled = true;
					rejectStart(new Error(this.exitInfo));
				}
			});

			const timer = setTimeout(() => {
				if (!settled) {
					settled = true;
					rejectStart(new Error("gorlm ready handshake timed out"));
					proc.kill();
				}
			}, PROTOCOL_TIMEOUT_MS);
			const wrappedResolve = () => {
				clearTimeout(timer);
				if (!settled) {
					settled = true;
					resolveStart();
				}
			};
			// patch: hand the wrapped resolver to onEvent via closure flag
			this.resolveStart = wrappedResolve;
		});
		return this.ready;
	}

	private resolveStart: () => void = () => {};

	private onEvent(line: string, resolveStart?: () => void) {
		let ev: any;
		try {
			ev = JSON.parse(line);
		} catch {
			return;
		}
		switch (ev.event) {
			case "ready":
				resolveStart?.();
				this.resolveStart();
				break;
			case "stdout":
			case "stderr": {
				const p = ev.id ? this.pending.get(ev.id) : undefined;
				if (p && ev.text) p.output.push(ev.text);
				break;
			}
			case "result": {
				const p = ev.id ? this.pending.get(ev.id) : undefined;
				if (p) p.result = ev.text;
				break;
			}
			case "error": {
				const p = ev.id ? this.pending.get(ev.id) : undefined;
				if (p) p.error = { ename: ev.ename ?? "Error", evalue: ev.evalue ?? "" };
				break;
			}
			case "host_request":
				this.onHostRequest(ev.id, ev.data);
				break;
			case "done": {
				const p = ev.id ? this.pending.get(ev.id) : undefined;
				if (p) {
					this.pending.delete(ev.id);
					p.resolve();
				}
				break;
			}
		}
	}

	/** v1: unbridged host kinds get an error reply; cells can handle it. */
	private onHostRequest(id: string, data: any) {
		const kind = typeof data?.kind === "string" ? data.kind : "unknown";
		this.writer?.(
			JSON.stringify({
				type: "host_reply",
				id,
				data: { status: "error", error: `go-kernel extension: host kind "${kind}" is not bridged yet` },
			}),
		);
	}

	async execute(code: string, signal?: AbortSignal): Promise<PendingRequest & { interrupted: boolean }> {
		await this.start();
		const id = randomUUID().replace(/-/g, "").slice(0, 16);
		let interrupted = false;
		const p: PendingRequest & { interrupted: boolean } = {
			output: [],
			resolve: () => {},
			interrupted,
		};
		const done = new Promise<void>((r) => (p.resolve = r));
		this.pending.set(id, p);

		const onAbort = () => {
			interrupted = true;
			p.interrupted = true;
			this.writer?.(JSON.stringify({ type: "interrupt", id }));
		};
		signal?.addEventListener("abort", onAbort, { once: true });
		const timer = setTimeout(() => onAbort(), CELL_TIMEOUT_MS);

		this.writer?.(JSON.stringify({ type: "execute", id, code }));
		await done;

		clearTimeout(timer);
		signal?.removeEventListener("abort", onAbort);
		if (this.exitInfo && p.error === undefined) {
			p.error = { ename: "KernelDied", evalue: this.exitInfo };
		}
		return p;
	}

	stop() {
		this.writer?.(JSON.stringify({ type: "shutdown" }));
		this.proc?.stdin?.end();
		setTimeout(() => this.proc?.kill("SIGKILL"), 3000);
	}
}

let kernel: GoKernel | null = null;

export default function goKernelExtension(pi: ExtensionAPI) {
	pi.on("session_start", async (_event, ctx) => {
		kernel = new GoKernel(kernelBinary());
	});

	pi.on("tool_call", async (event) => {
		if (event.toolName === "ipython") {
			return {
				block: true,
				reason:
					"The Python kernel is disabled in this repository. Use the `go` tool instead — cells are Go source in a persistent interpreter.",
			};
		}
	});

	pi.registerTool(
		defineTool({
			name: "go",
			label: "Go",
			description:
				"Execute Go source in a persistent interpreter kernel (Yaegi). State persists across calls: variables, functions, types. Use this for all code execution, computation, and orchestration instead of the Python REPL.",
			parameters: Type.Object({
				code: Type.String({ description: "Go source. May contain imports, declarations, statements; the trailing expression value is returned." }),
			}),
			promptGuidelines: [
				"Use the `go` tool for code execution: cells are Go source, not Python — no `await`, no `pip`; use goroutines and channels.",
				"Go cell state persists across `go` tool calls (`x := 40` in one cell, `x + 2` in the next works).",
				"Inside Go cells, `import \"rlm/rlm\"` binds runtime helpers: `rlm.Sleep(ms)` (interruptible) and `rlm.HostCall(kind, payload)` (host bridge).",
				"Prefer `rlm.Sleep` over `time.Sleep` in cells so interrupts cancel promptly.",
			],
			async execute(_toolCallId, params, signal) {
				if (!kernel) return { content: [{ type: "text", text: "go-kernel: no session" }] };
				try {
					const r = await kernel.execute(params.code, signal);
					const parts: string[] = [];
					if (r.output.length) parts.push(r.output.join(""));
					if (r.result !== undefined) parts.push(`=> ${r.result}\n`);
					if (r.error) parts.push(`${r.error.ename}: ${r.error.evalue}\n`);
					if (r.interrupted) parts.push("(interrupted)\n");
					return { content: [{ type: "text", text: parts.join("") || "(no output)" }] };
				} catch (e: any) {
					return { content: [{ type: "text", text: `go-kernel error: ${e?.message ?? e}` }] };
				}
			},
		}),
	);
}
