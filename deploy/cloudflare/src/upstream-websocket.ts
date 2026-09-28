// A client WebSocket to an upstream with upgrade headers, in the shape ocx's CodexWsSession drives
// (Bun's `new WebSocket(url, { headers })`). Workers cannot set headers on `new WebSocket`, so this
// dials with a fetch carrying `Upgrade: websocket` and re-dispatches the accepted socket's events.

const CONNECTING = 0;
const OPEN = 1;
const CLOSING = 2;
const CLOSED = 3;

class UpstreamWebSocket extends EventTarget {
  readyState = CONNECTING;
  private socket: WebSocket | undefined;
  private readonly queued: (string | ArrayBuffer)[] = [];
  private closeRequested: { code?: number; reason?: string } | undefined;
  private readonly dial = new AbortController();

  constructor(url: string, headers: Record<string, string>) {
    super();
    const httpUrl = url.replace(/^wss:/i, "https:").replace(/^ws:/i, "http:");
    fetch(httpUrl, { headers: { ...headers, upgrade: "websocket" }, signal: this.dial.signal }).then(response => {
      const socket = response.webSocket;
      if (!socket) {
        void response.body?.cancel();
        this.fail();
        return;
      }
      socket.accept();
      this.socket = socket;
      socket.addEventListener("message", event => this.dispatchEvent(new MessageEvent("message", { data: event.data })));
      socket.addEventListener("close", event => {
        // Complete the closing handshake; Workers do not answer a peer's close on their own.
        if (this.readyState !== CLOSED) {
          try { socket.close(event.code === 1005 ? 1000 : event.code, event.reason); } catch { /* already closed */ }
        }
        this.readyState = CLOSED;
        this.dispatchEvent(new CloseEvent("close", { code: event.code, reason: event.reason, wasClean: event.wasClean }));
      });
      socket.addEventListener("error", () => this.dispatchEvent(new Event("error")));
      if (this.closeRequested) {
        socket.close(this.closeRequested.code ?? 1000, this.closeRequested.reason);
        return;
      }
      this.readyState = OPEN;
      this.dispatchEvent(new Event("open"));
      for (const data of this.queued.splice(0)) socket.send(data);
    }, () => this.fail());
  }

  send(data: string | ArrayBuffer): void {
    if (this.readyState === CONNECTING) this.queued.push(data);
    else if (this.readyState === OPEN) this.socket!.send(data);
  }

  close(code?: number, reason?: string): void {
    if (this.readyState === CLOSED || this.readyState === CLOSING) return;
    this.readyState = CLOSING;
    if (this.socket) this.socket.close(code ?? 1000, reason);
    else {
      this.closeRequested = { code, reason };
      this.dial.abort();
    }
  }

  /** A refused or failed upgrade: an error before any open, then a close, as a WebSocket reports it. */
  private fail(): void {
    this.readyState = CLOSED;
    this.dispatchEvent(new Event("error"));
    this.dispatchEvent(new CloseEvent("close", { code: 1006, reason: "upgrade failed", wasClean: false }));
  }
}

export function openUpstreamWebSocket(url: string, headers: Record<string, string>): WebSocket {
  return new UpstreamWebSocket(url, headers) as unknown as WebSocket;
}
