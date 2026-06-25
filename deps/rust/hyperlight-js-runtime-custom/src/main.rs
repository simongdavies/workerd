/*
A custom hyperlight-js guest runtime that bakes a WinterTC ("Minimum Common API") global surface
into QuickJS via the `custom_globals!` extension mechanism. Built out of band (`cargo hyperlight
build`) into the prebuilt blob workerd embeds (//deps/rust/hyperlight-js-runtime:jsruntime.bin).

Two kinds of globals are installed:
  - Rust classes (via #[rquickjs::class]) for byte-level APIs: TextEncoder / TextDecoder.
  - JS polyfills (via ctx.eval) for the rest: atob / btoa, URL / URLSearchParams, Headers,
    DOMException, Event / EventTarget, AbortController / AbortSignal, structuredClone,
    queueMicrotask, Blob.

The same binary builds as a native CLI (for fast local testing) and as a Hyperlight guest; the lib
provides all guest infrastructure, so this file only declares the globals + a CLI entry point.
*/
#![cfg_attr(hyperlight, no_std)]
#![cfg_attr(hyperlight, no_main)]

// The hyperlight (guest) target is no_std: owned heap types come from `alloc`, not the std prelude.
#[cfg(hyperlight)]
extern crate alloc;
#[cfg(hyperlight)]
use alloc::string::String;

use rquickjs::class::Trace;
use rquickjs::{Class, Ctx, JsLifetime, TypedArray};

// No custom native modules; the built-ins (io, crypto, console, require) are inherited.
hyperlight_js_runtime::native_modules! {}

// ── TextEncoder (WHATWG Encoding) ──────────────────────────────────────────
#[rquickjs::class]
#[derive(Trace, JsLifetime)]
pub struct TextEncoder {}

#[rquickjs::methods]
impl TextEncoder {
    #[qjs(constructor)]
    pub fn new() -> Self {
        TextEncoder {}
    }

    // Encode a string to UTF-8 bytes, returned as a Uint8Array.
    pub fn encode<'js>(
        &self,
        ctx: Ctx<'js>,
        input: String,
    ) -> rquickjs::Result<TypedArray<'js, u8>> {
        TypedArray::new(ctx, input.into_bytes())
    }
}

// ── TextDecoder (WHATWG Encoding, utf-8, lossy) ─────────────────────────────
#[rquickjs::class]
#[derive(Trace, JsLifetime)]
pub struct TextDecoder {}

#[rquickjs::methods]
impl TextDecoder {
    #[qjs(constructor)]
    pub fn new() -> Self {
        TextDecoder {}
    }

    // Decode UTF-8 bytes (a Uint8Array) to a string; invalid sequences become U+FFFD.
    pub fn decode(&self, input: TypedArray<'_, u8>) -> String {
        let bytes: &[u8] = input.as_ref();
        String::from_utf8_lossy(bytes).into_owned()
    }
}

// ── Global installation ─────────────────────────────────────────────────────
fn setup_wintertc(ctx: &Ctx<'_>) -> rquickjs::Result<()> {
    // Rust-class globals: `Class::define` builds the constructor and installs it on globalThis
    // under the class's name (so `new TextEncoder()` / `new TextDecoder()` work with no import).
    Class::<TextEncoder>::define(&ctx.globals())?;
    Class::<TextDecoder>::define(&ctx.globals())?;

    // JS-polyfill globals: base64 (atob / btoa), per the HTML "forgiving-base64" semantics over
    // Latin-1 binary strings.
    ctx.eval::<(), _>(BASE64_POLYFILL)?;

    // URL + URLSearchParams (one closure: searchParams stays live-bound to the URL's query).
    ctx.eval::<(), _>(URL_POLYFILL)?;

    // Headers (WHATWG Fetch): case-insensitive, combine-on-append, Set-Cookie kept separate.
    ctx.eval::<(), _>(HEADERS_POLYFILL)?;

    // DOMException + Event/EventTarget + AbortController/AbortSignal (one closure: AbortSignal is an
    // EventTarget and its default abort reason is an AbortError DOMException).
    ctx.eval::<(), _>(EVENTS_POLYFILL)?;

    // Blob (in-memory, UTF-8-backed) + structuredClone (cycle-aware deep clone) + queueMicrotask
    // (maps to the QuickJS job queue).
    ctx.eval::<(), _>(UTIL_POLYFILL)?;

    Ok(())
}

const BASE64_POLYFILL: &str = r#"
(() => {
  const CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  globalThis.btoa = function btoa(data) {
    const str = String(data);
    let out = "";
    for (let i = 0; i < str.length; i += 3) {
      const c0 = str.charCodeAt(i);
      const c1 = str.charCodeAt(i + 1);
      const c2 = str.charCodeAt(i + 2);
      if (c0 > 0xff || (i + 1 < str.length && c1 > 0xff) || (i + 2 < str.length && c2 > 0xff)) {
        throw new Error("btoa: argument contains characters outside the Latin1 range");
      }
      const e0 = c0 >> 2;
      const e1 = ((c0 & 3) << 4) | (Number.isNaN(c1) ? 0 : c1 >> 4);
      out += CHARS[e0] + CHARS[e1];
      out += Number.isNaN(c1) ? "=" : CHARS[((c1 & 15) << 2) | (Number.isNaN(c2) ? 0 : c2 >> 6)];
      out += Number.isNaN(c2) ? "=" : CHARS[c2 & 63];
    }
    return out;
  };

  globalThis.atob = function atob(data) {
    let str = String(data).replace(/[ \t\n\f\r]/g, "");
    if (str.length % 4 === 1) {
      throw new Error("atob: invalid base64 length");
    }
    str = str.replace(/=+$/, "");
    let out = "";
    let bits = 0;
    let nbits = 0;
    for (let i = 0; i < str.length; i++) {
      const idx = CHARS.indexOf(str[i]);
      if (idx === -1) {
        throw new Error("atob: invalid base64 character");
      }
      bits = (bits << 6) | idx;
      nbits += 6;
      if (nbits >= 8) {
        nbits -= 8;
        out += String.fromCharCode((bits >> nbits) & 0xff);
      }
    }
    return out;
  };
})();
"#;

// URL + URLSearchParams. A pragmatic RFC-3986-style parser (not a full WHATWG state machine): it
// covers absolute URLs, the common relative-resolution cases, default-port stripping, and a live
// `url.searchParams` binding. Both classes share one closure so URLSearchParams mutations can write
// back through to the owning URL's query without leaking internal properties.
const URL_POLYFILL: &str = r##"
(() => {
  // Private state shared across URL and URLSearchParams (same closure) so url.searchParams stays
  // live-bound to the URL's query without leaking enumerable properties.
  const spList = new WeakMap();    // URLSearchParams -> Array<[name, value]>
  const spSync = new WeakMap();    // URLSearchParams -> (queryString) => void  (writes back to a URL)
  const urlState = new WeakMap();  // URL -> parsed component record
  const urlParams = new WeakMap(); // URL -> its cached URLSearchParams

  // Special schemes and their default ports (the default is stripped from the serialization).
  const SPECIAL = { "http:": 80, "https:": 443, "ws:": 80, "wss:": 443, "ftp:": 21, "file:": null };

  // --- application/x-www-form-urlencoded helpers ---------------------------
  function decode(s) { try { return decodeURIComponent(String(s).replace(/\+/g, " ")); } catch (e) { return String(s); } }
  function encode(s) { return encodeURIComponent(String(s)).replace(/%20/g, "+"); }
  function parseQuery(input) {
    const list = [];
    let s = String(input);
    if (s.startsWith("?")) s = s.slice(1);
    if (s === "") return list;
    for (const piece of s.split("&")) {
      if (piece === "") continue;
      const eq = piece.indexOf("=");
      if (eq === -1) list.push([decode(piece), ""]);
      else list.push([decode(piece.slice(0, eq)), decode(piece.slice(eq + 1))]);
    }
    return list;
  }
  function serialize(list) { return list.map(p => encode(p[0]) + "=" + encode(p[1])).join("&"); }
  function notify(sp) { const fn = spSync.get(sp); if (fn) fn(serialize(spList.get(sp))); }

  // --- URLSearchParams (WHATWG URL) ----------------------------------------
  class URLSearchParams {
    constructor(init) {
      let list = [];
      if (init === undefined || init === null || init === "") {
        list = [];
      } else if (typeof init === "string") {
        list = parseQuery(init);
      } else if (init instanceof URLSearchParams) {
        list = spList.get(init).map(p => [p[0], p[1]]);
      } else if (typeof init[Symbol.iterator] === "function") {
        for (const pair of init) {
          const arr = Array.from(pair);
          if (arr.length !== 2) throw new TypeError("URLSearchParams: each entry must be a [name, value] pair");
          list.push([String(arr[0]), String(arr[1])]);
        }
      } else if (typeof init === "object") {
        for (const key of Object.keys(init)) list.push([key, String(init[key])]);
      }
      spList.set(this, list);
    }
    append(name, value) { spList.get(this).push([String(name), String(value)]); notify(this); }
    delete(name) { name = String(name); const l = spList.get(this); for (let i = l.length - 1; i >= 0; i--) if (l[i][0] === name) l.splice(i, 1); notify(this); }
    get(name) { name = String(name); for (const p of spList.get(this)) if (p[0] === name) return p[1]; return null; }
    getAll(name) { name = String(name); return spList.get(this).filter(p => p[0] === name).map(p => p[1]); }
    has(name) { name = String(name); return spList.get(this).some(p => p[0] === name); }
    set(name, value) {
      // Per WHATWG: set the FIRST matching pair's value (keeping its position) and drop the rest.
      name = String(name); value = String(value);
      const l = spList.get(this); let seen = false;
      for (let i = 0; i < l.length;) {
        if (l[i][0] === name) {
          if (!seen) { l[i][1] = value; seen = true; i++; }
          else { l.splice(i, 1); }
        } else i++;
      }
      if (!seen) l.push([name, value]);
      notify(this);
    }
    sort() { spList.get(this).sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0)); notify(this); }
    forEach(cb, thisArg) { for (const p of spList.get(this).slice()) cb.call(thisArg, p[1], p[0], this); }
    keys() { return spList.get(this).map(p => p[0])[Symbol.iterator](); }
    values() { return spList.get(this).map(p => p[1])[Symbol.iterator](); }
    entries() { return spList.get(this).map(p => [p[0], p[1]])[Symbol.iterator](); }
    [Symbol.iterator]() { return this.entries(); }
    get size() { return spList.get(this).length; }
    toString() { return serialize(spList.get(this)); }
  }

  // --- URL parsing (pragmatic RFC-3986 split) ------------------------------
  const URL_RE = /^(?:([a-zA-Z][a-zA-Z0-9+.-]*):)?(?:\/\/([^\/?#]*))?([^?#]*)(?:\?([^#]*))?(?:#(.*))?$/;

  function splitAuthority(authority) {
    let userinfo = "", host = authority;
    const at = authority.lastIndexOf("@");
    if (at !== -1) { userinfo = authority.slice(0, at); host = authority.slice(at + 1); }
    let username = "", password = "";
    if (userinfo) { const c = userinfo.indexOf(":"); if (c === -1) username = userinfo; else { username = userinfo.slice(0, c); password = userinfo.slice(c + 1); } }
    let hostname = host, port = "";
    if (host.startsWith("[")) { const close = host.indexOf("]"); hostname = host.slice(0, close + 1); const rest = host.slice(close + 1); if (rest.startsWith(":")) port = rest.slice(1); }
    else { const c = host.lastIndexOf(":"); if (c !== -1) { hostname = host.slice(0, c); port = host.slice(c + 1); } }
    return { username: username, password: password, hostname: hostname, port: port };
  }

  // RFC 3986 section 5.2.4 remove_dot_segments.
  function removeDotSegments(path) {
    let input = String(path), output = "";
    while (input.length > 0) {
      if (input.startsWith("../")) input = input.slice(3);
      else if (input.startsWith("./")) input = input.slice(2);
      else if (input.startsWith("/./")) input = "/" + input.slice(3);
      else if (input === "/.") input = "/";
      else if (input.startsWith("/../")) { input = "/" + input.slice(4); const i = output.lastIndexOf("/"); output = i >= 0 ? output.slice(0, i) : ""; }
      else if (input === "/..") { input = "/"; const i = output.lastIndexOf("/"); output = i >= 0 ? output.slice(0, i) : ""; }
      else if (input === "." || input === "..") input = "";
      else {
        let i = input.startsWith("/") ? input.indexOf("/", 1) : input.indexOf("/");
        if (i === -1) i = input.length;
        output += input.slice(0, i); input = input.slice(i);
      }
    }
    return output;
  }

  function finalize(st) {
    if (Object.prototype.hasOwnProperty.call(SPECIAL, st.scheme) && st.port !== "" && String(SPECIAL[st.scheme]) === st.port) st.port = "";
    if (Object.prototype.hasOwnProperty.call(SPECIAL, st.scheme) && st.hasAuthority && st.path === "") st.path = "/";
    return st;
  }

  function parseAbsolute(input) {
    const s = String(input).replace(/[\t\n\r]/g, "").trim();
    const m = URL_RE.exec(s);
    if (!m || !m[1]) return null;
    const st = {
      scheme: m[1].toLowerCase() + ":",
      hasAuthority: m[2] !== undefined,
      username: "", password: "", hostname: "", port: "",
      path: m[3] || "", query: m[4] === undefined ? "" : m[4], fragment: m[5] === undefined ? "" : m[5],
    };
    if (m[2] !== undefined) { const a = splitAuthority(m[2]); st.username = a.username; st.password = a.password; st.hostname = a.hostname.toLowerCase(); st.port = a.port; }
    return finalize(st);
  }

  function resolve(input, base) {
    const s = String(input).replace(/[\t\n\r]/g, "").trim();
    const m = URL_RE.exec(s);
    if (m && m[1]) return parseAbsolute(s);
    const st = { scheme: base.scheme, hasAuthority: base.hasAuthority, username: base.username, password: base.password, hostname: base.hostname, port: base.port, path: base.path, query: base.query, fragment: base.fragment };
    const authority = m ? m[2] : undefined;
    const path = m ? (m[3] || "") : "";
    const query = m ? m[4] : undefined;
    const fragment = m ? m[5] : undefined;
    if (authority !== undefined) {
      const a = splitAuthority(authority);
      st.hasAuthority = true; st.username = a.username; st.password = a.password; st.hostname = a.hostname.toLowerCase(); st.port = a.port;
      st.path = removeDotSegments(path); st.query = query === undefined ? "" : query; st.fragment = fragment === undefined ? "" : fragment;
    } else if (path === "") {
      if (query !== undefined) { st.query = query; st.fragment = fragment === undefined ? "" : fragment; }
      else if (fragment !== undefined) { st.fragment = fragment; }
    } else if (path.startsWith("/")) {
      st.path = removeDotSegments(path); st.query = query === undefined ? "" : query; st.fragment = fragment === undefined ? "" : fragment;
    } else {
      const baseDir = base.path.slice(0, base.path.lastIndexOf("/") + 1);
      st.path = removeDotSegments(baseDir + path); st.query = query === undefined ? "" : query; st.fragment = fragment === undefined ? "" : fragment;
    }
    return finalize(st);
  }

  function buildHref(st) {
    let h = st.scheme;
    if (st.hasAuthority) {
      h += "//";
      if (st.username) { h += st.username; if (st.password) h += ":" + st.password; h += "@"; }
      h += st.hostname;
      if (st.port) h += ":" + st.port;
    }
    h += st.path;
    if (st.query) h += "?" + st.query;
    if (st.fragment) h += "#" + st.fragment;
    return h;
  }

  function originOf(st) {
    if (st.scheme === "file:") return "null";
    if (Object.prototype.hasOwnProperty.call(SPECIAL, st.scheme)) return st.scheme + "//" + st.hostname + (st.port ? ":" + st.port : "");
    return "null";
  }

  class URL {
    constructor(url, base) {
      let st;
      if (base !== undefined && base !== null) {
        const baseState = base instanceof URL ? urlState.get(base) : parseAbsolute(base);
        if (!baseState) throw new TypeError("Invalid base URL: " + base);
        st = resolve(url, baseState);
      } else {
        st = parseAbsolute(url);
      }
      if (!st) throw new TypeError("Invalid URL: " + url);
      urlState.set(this, st);
    }
    get href() { return buildHref(urlState.get(this)); }
    set href(value) { const st = parseAbsolute(value); if (!st) throw new TypeError("Invalid URL: " + value); urlState.set(this, st); const sp = urlParams.get(this); if (sp) spList.set(sp, parseQuery(st.query)); }
    toString() { return this.href; }
    toJSON() { return this.href; }
    get protocol() { return urlState.get(this).scheme; }
    set protocol(v) { v = String(v).replace(/:+$/, "").toLowerCase(); if (/^[a-z][a-z0-9+.-]*$/.test(v)) urlState.get(this).scheme = v + ":"; }
    get username() { return urlState.get(this).username; }
    set username(v) { urlState.get(this).username = String(v); }
    get password() { return urlState.get(this).password; }
    set password(v) { urlState.get(this).password = String(v); }
    get host() { const st = urlState.get(this); return st.hostname + (st.port ? ":" + st.port : ""); }
    set host(v) { const a = splitAuthority(String(v)); const st = urlState.get(this); st.hostname = a.hostname.toLowerCase(); st.port = a.port; finalize(st); }
    get hostname() { return urlState.get(this).hostname; }
    set hostname(v) { urlState.get(this).hostname = String(v).toLowerCase(); }
    get port() { return urlState.get(this).port; }
    set port(v) { v = String(v); const st = urlState.get(this); if (v === "") { st.port = ""; return; } if (/^[0-9]+$/.test(v)) { st.port = String(parseInt(v, 10)); finalize(st); } }
    get pathname() { return urlState.get(this).path; }
    set pathname(v) { v = String(v); const st = urlState.get(this); if (st.hasAuthority && v !== "" && !v.startsWith("/")) v = "/" + v; st.path = v; }
    get search() { const q = urlState.get(this).query; return q ? "?" + q : ""; }
    set search(v) { v = String(v); if (v.startsWith("?")) v = v.slice(1); const st = urlState.get(this); st.query = v; const sp = urlParams.get(this); if (sp) spList.set(sp, parseQuery(v)); }
    get searchParams() {
      let sp = urlParams.get(this);
      if (!sp) {
        const self = this;
        sp = new URLSearchParams(urlState.get(this).query);
        spSync.set(sp, function (qs) { urlState.get(self).query = qs; });
        urlParams.set(this, sp);
      }
      return sp;
    }
    get hash() { const f = urlState.get(this).fragment; return f ? "#" + f : ""; }
    set hash(v) { v = String(v); if (v.startsWith("#")) v = v.slice(1); urlState.get(this).fragment = v; }
    get origin() { return originOf(urlState.get(this)); }
  }

  globalThis.URL = URL;
  globalThis.URLSearchParams = URLSearchParams;
})();
"##;

// Headers (WHATWG Fetch). Map-backed and case-insensitive: append combines values with ", ",
// iteration is sorted by lowercased name, and Set-Cookie is kept as a separate list (so
// getSetCookie() can return the individual cookies rather than a comma-joined blob).
const HEADERS_POLYFILL: &str = r##"
(() => {
  const store = new WeakMap();   // Headers -> Map<lowerName, combinedValue>
  const cookies = new WeakMap(); // Headers -> Array<string> (Set-Cookie kept separate)

  const TOKEN_RE = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
  function normName(name) {
    name = String(name);
    if (name === "" || !TOKEN_RE.test(name)) throw new TypeError("Invalid header name: '" + name + "'");
    return name.toLowerCase();
  }
  function normValue(value) {
    value = String(value).replace(/^[\t ]+/, "").replace(/[\t ]+$/, "");
    if (/[\0\r\n]/.test(value)) throw new TypeError("Invalid header value");
    return value;
  }

  class Headers {
    constructor(init) {
      store.set(this, new Map());
      cookies.set(this, []);
      if (init === undefined || init === null) return;
      if (init instanceof Headers) {
        for (const pair of init.entries()) this.append(pair[0], pair[1]);
      } else if (Array.isArray(init) || typeof init[Symbol.iterator] === "function") {
        for (const pair of init) { const arr = Array.from(pair); if (arr.length !== 2) throw new TypeError("Headers: init entry must be a [name, value] pair"); this.append(arr[0], arr[1]); }
      } else if (typeof init === "object") {
        for (const key of Object.keys(init)) this.append(key, init[key]);
      }
    }
    append(name, value) {
      const n = normName(name), v = normValue(value);
      if (n === "set-cookie") { cookies.get(this).push(v); return; }
      const m = store.get(this);
      m.set(n, m.has(n) ? m.get(n) + ", " + v : v);
    }
    set(name, value) {
      const n = normName(name), v = normValue(value);
      if (n === "set-cookie") { cookies.set(this, [v]); return; }
      store.get(this).set(n, v);
    }
    get(name) {
      const n = normName(name);
      if (n === "set-cookie") { const c = cookies.get(this); return c.length ? c.join(", ") : null; }
      const m = store.get(this); return m.has(n) ? m.get(n) : null;
    }
    getSetCookie() { return cookies.get(this).slice(); }
    has(name) { const n = normName(name); if (n === "set-cookie") return cookies.get(this).length > 0; return store.get(this).has(n); }
    delete(name) { const n = normName(name); if (n === "set-cookie") { cookies.set(this, []); return; } store.get(this).delete(n); }
    forEach(cb, thisArg) { for (const pair of this.entries()) cb.call(thisArg, pair[1], pair[0], this); }
    entries() {
      const m = store.get(this);
      const all = [];
      for (const n of m.keys()) all.push([n, m.get(n)]);
      for (const c of cookies.get(this)) all.push(["set-cookie", c]);
      all.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
      return all[Symbol.iterator]();
    }
    keys() { const out = []; for (const p of this.entries()) out.push(p[0]); return out[Symbol.iterator](); }
    values() { const out = []; for (const p of this.entries()) out.push(p[1]); return out[Symbol.iterator](); }
    [Symbol.iterator]() { return this.entries(); }
  }

  globalThis.Headers = Headers;
})();
"##;

// DOMException + Event / EventTarget + AbortController / AbortSignal. One closure: AbortSignal is an
// EventTarget, and the default abort reason is an AbortError DOMException, so they share the same
// private maps. Note: AbortSignal.timeout() cannot actually fire in this execution model (the vCPU is
// parked between calls), so it returns a never-aborting signal rather than pretending to schedule.
const EVENTS_POLYFILL: &str = r##"
(() => {
  // ---- DOMException (WHATWG) ----
  const DOM_CODES = {
    IndexSizeError: 1, HierarchyRequestError: 3, WrongDocumentError: 4, InvalidCharacterError: 5,
    NoModificationAllowedError: 7, NotFoundError: 8, NotSupportedError: 9, InUseAttributeError: 10,
    InvalidStateError: 11, SyntaxError: 12, InvalidModificationError: 13, NamespaceError: 14,
    InvalidAccessError: 15, SecurityError: 18, NetworkError: 19, AbortError: 20, URLMismatchError: 21,
    QuotaExceededError: 22, TimeoutError: 23, InvalidNodeTypeError: 24, DataCloneError: 25,
  };
  class DOMException extends Error {
    constructor(message, name) {
      super(message === undefined ? "" : String(message));
      Object.defineProperty(this, "name", { value: name === undefined ? "Error" : String(name), writable: true, configurable: true });
    }
    get code() { return DOM_CODES[this.name] || 0; }
    get [Symbol.toStringTag]() { return "DOMException"; }
  }
  const DOM_CONSTS = {
    INDEX_SIZE_ERR: 1, DOMSTRING_SIZE_ERR: 2, HIERARCHY_REQUEST_ERR: 3, WRONG_DOCUMENT_ERR: 4,
    INVALID_CHARACTER_ERR: 5, NO_DATA_ALLOWED_ERR: 6, NO_MODIFICATION_ALLOWED_ERR: 7, NOT_FOUND_ERR: 8,
    NOT_SUPPORTED_ERR: 9, INUSE_ATTRIBUTE_ERR: 10, INVALID_STATE_ERR: 11, SYNTAX_ERR: 12,
    INVALID_MODIFICATION_ERR: 13, NAMESPACE_ERR: 14, INVALID_ACCESS_ERR: 15, VALIDATION_ERR: 16,
    TYPE_MISMATCH_ERR: 17, SECURITY_ERR: 18, NETWORK_ERR: 19, ABORT_ERR: 20, URL_MISMATCH_ERR: 21,
    QUOTA_EXCEEDED_ERR: 22, TIMEOUT_ERR: 23, INVALID_NODE_TYPE_ERR: 24, DATA_CLONE_ERR: 25,
  };
  for (const k of Object.keys(DOM_CONSTS)) {
    Object.defineProperty(DOMException, k, { value: DOM_CONSTS[k], enumerable: true });
    Object.defineProperty(DOMException.prototype, k, { value: DOM_CONSTS[k], enumerable: true });
  }

  // ---- Event ----
  const evState = new WeakMap(); // Event -> { stopImmediate, stopProp, dispatched }
  class Event {
    constructor(type, init) {
      init = init || {};
      this.type = String(type);
      this.bubbles = !!init.bubbles;
      this.cancelable = !!init.cancelable;
      this.composed = !!init.composed;
      this.defaultPrevented = false;
      this.target = null;
      this.currentTarget = null;
      this.srcElement = null;
      this.eventPhase = 0;
      this.isTrusted = false;
      this.timeStamp = 0; // no in-guest clock yet
      evState.set(this, { stopImmediate: false, stopProp: false, dispatched: false });
    }
    preventDefault() { if (this.cancelable) this.defaultPrevented = true; }
    stopPropagation() { evState.get(this).stopProp = true; }
    stopImmediatePropagation() { const s = evState.get(this); s.stopImmediate = true; s.stopProp = true; }
    composedPath() { return this.currentTarget ? [this.currentTarget] : []; }
    get returnValue() { return !this.defaultPrevented; }
    set returnValue(v) { if (this.cancelable && v === false) this.defaultPrevented = true; }
  }
  Object.defineProperties(Event, { NONE: { value: 0 }, CAPTURING_PHASE: { value: 1 }, AT_TARGET: { value: 2 }, BUBBLING_PHASE: { value: 3 } });

  // ---- EventTarget ----
  const listeners = new WeakMap(); // EventTarget -> Map<type, Array<entry>>
  class EventTarget {
    constructor() { listeners.set(this, new Map()); }
    addEventListener(type, callback, options) {
      if (callback === undefined || callback === null) return;
      type = String(type);
      const opts = (typeof options === "boolean") ? { capture: options } : (options || {});
      const capture = !!opts.capture, once = !!opts.once, passive = !!opts.passive, signal = opts.signal || null;
      if (signal && signal.aborted) return;
      const map = listeners.get(this);
      if (!map.has(type)) map.set(type, []);
      const arr = map.get(type);
      for (const l of arr) if (l.callback === callback && l.capture === capture) return;
      arr.push({ callback: callback, capture: capture, once: once, passive: passive, signal: signal });
      if (signal) { const self = this; signal.addEventListener("abort", function () { self.removeEventListener(type, callback, { capture: capture }); }, { once: true }); }
    }
    removeEventListener(type, callback, options) {
      type = String(type);
      const capture = (typeof options === "boolean") ? options : !!(options && options.capture);
      const map = listeners.get(this); if (!map.has(type)) return;
      const arr = map.get(type);
      for (let i = 0; i < arr.length; i++) if (arr[i].callback === callback && arr[i].capture === capture) { arr.splice(i, 1); return; }
    }
    dispatchEvent(event) {
      const st = evState.get(event);
      if (st && st.dispatched) throw new DOMException("The event is already being dispatched", "InvalidStateError");
      if (st) st.dispatched = true;
      const map = listeners.get(this);
      event.target = this; event.srcElement = this; event.currentTarget = this; event.eventPhase = 2;
      const arr = (map.get(event.type) || []).slice();
      for (const l of arr) {
        if (st && st.stopImmediate) break;
        try {
          if (typeof l.callback === "function") l.callback.call(this, event);
          else if (l.callback && typeof l.callback.handleEvent === "function") l.callback.handleEvent(event);
        } catch (e) { console.error(e); }
        if (l.once) this.removeEventListener(event.type, l.callback, { capture: l.capture });
      }
      event.currentTarget = null; event.eventPhase = 0;
      if (st) { st.dispatched = false; st.stopImmediate = false; st.stopProp = false; }
      return !event.defaultPrevented;
    }
  }

  // ---- AbortSignal / AbortController ----
  const signalState = new WeakMap();      // AbortSignal -> { aborted, reason, onabort }
  const controllerSignal = new WeakMap(); // AbortController -> AbortSignal
  let allowConstruct = false;
  function newSignal() { allowConstruct = true; try { return new AbortSignal(); } finally { allowConstruct = false; } }
  function doAbort(signal, reason) {
    const s = signalState.get(signal);
    if (s.aborted) return;
    s.aborted = true;
    s.reason = (reason !== undefined) ? reason : new DOMException("This operation was aborted", "AbortError");
    const ev = new Event("abort");
    if (typeof s.onabort === "function") { try { s.onabort.call(signal, ev); } catch (e) { console.error(e); } }
    signal.dispatchEvent(ev);
  }
  class AbortSignal extends EventTarget {
    constructor() {
      super();
      if (!allowConstruct) throw new TypeError("Illegal constructor");
      signalState.set(this, { aborted: false, reason: undefined, onabort: null });
    }
    get aborted() { return signalState.get(this).aborted; }
    get reason() { return signalState.get(this).reason; }
    get onabort() { return signalState.get(this).onabort; }
    set onabort(fn) { signalState.get(this).onabort = (typeof fn === "function") ? fn : null; }
    throwIfAborted() { const s = signalState.get(this); if (s.aborted) throw s.reason; }
    static abort(reason) { const sig = newSignal(); const s = signalState.get(sig); s.aborted = true; s.reason = (reason !== undefined) ? reason : new DOMException("This operation was aborted", "AbortError"); return sig; }
    static timeout(ms) { return newSignal(); }
    static any(signals) {
      const sig = newSignal();
      const list = Array.from(signals);
      for (const s of list) { if (s.aborted) { const st = signalState.get(sig); st.aborted = true; st.reason = s.reason; return sig; } }
      for (const s of list) { s.addEventListener("abort", function () { doAbort(sig, s.reason); }, { once: true }); }
      return sig;
    }
  }
  class AbortController {
    constructor() { controllerSignal.set(this, newSignal()); }
    get signal() { return controllerSignal.get(this); }
    abort(reason) { doAbort(controllerSignal.get(this), reason); }
  }

  globalThis.DOMException = DOMException;
  globalThis.Event = Event;
  globalThis.EventTarget = EventTarget;
  globalThis.AbortSignal = AbortSignal;
  globalThis.AbortController = AbortController;
})();
"##;

// Blob + structuredClone + queueMicrotask. Blob is an in-memory byte container (UTF-8 for string
// parts) with async text()/arrayBuffer()/bytes() accessors; its stream() is unsupported (Tier 3).
// structuredClone is a cycle-aware deep clone over the structured-clone-able types (throws a
// DataCloneError for functions/symbols). queueMicrotask defers onto QuickJS's existing job queue.
const UTIL_POLYFILL: &str = r##"
(() => {
  const blobBytes = new WeakMap(); // Blob -> Uint8Array
  const blobType = new WeakMap();  // Blob -> string

  function encodeUtf8(s) { return new TextEncoder().encode(s); }
  function decodeUtf8(bytes) { return new TextDecoder().decode(bytes); }
  function toBytes(part) {
    if (part instanceof Blob) return blobBytes.get(part);
    if (part instanceof ArrayBuffer) return new Uint8Array(part.slice(0));
    if (ArrayBuffer.isView(part)) return new Uint8Array(part.buffer.slice(part.byteOffset, part.byteOffset + part.byteLength));
    return encodeUtf8(String(part));
  }

  class Blob {
    constructor(parts, options) {
      options = options || {};
      const chunks = [];
      if (parts !== undefined && parts !== null) {
        if (typeof parts[Symbol.iterator] !== "function") throw new TypeError("Blob parts must be iterable");
        for (const part of parts) chunks.push(toBytes(part));
      }
      let total = 0; for (const c of chunks) total += c.length;
      const bytes = new Uint8Array(total); let off = 0;
      for (const c of chunks) { bytes.set(c, off); off += c.length; }
      blobBytes.set(this, bytes);
      const type = String(options.type || "");
      blobType.set(this, /[^\u0020-\u007e]/.test(type) ? "" : type.toLowerCase());
    }
    get size() { return blobBytes.get(this).length; }
    get type() { return blobType.get(this); }
    slice(start, end, contentType) {
      const bytes = blobBytes.get(this); const len = bytes.length;
      const s = (start === undefined) ? 0 : (start < 0 ? Math.max(len + start, 0) : Math.min(start, len));
      const e = (end === undefined) ? len : (end < 0 ? Math.max(len + end, 0) : Math.min(end, len));
      const out = new Blob([], { type: contentType !== undefined ? String(contentType) : "" });
      blobBytes.set(out, bytes.slice(s, Math.max(s, e)));
      return out;
    }
    text() { return Promise.resolve(decodeUtf8(blobBytes.get(this))); }
    arrayBuffer() { const b = blobBytes.get(this); return Promise.resolve(b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength)); }
    bytes() { return Promise.resolve(blobBytes.get(this).slice()); }
    stream() { throw new DOMException("Blob.stream() is not supported in this runtime", "NotSupportedError"); }
    get [Symbol.toStringTag]() { return "Blob"; }
  }

  function structuredClone(value) {
    const seen = new Map();
    function clone(v) {
      if (v === null || (typeof v !== "object" && typeof v !== "function")) {
        if (typeof v === "symbol") throw new DOMException("Symbol could not be cloned", "DataCloneError");
        return v;
      }
      if (typeof v === "function") throw new DOMException("Function could not be cloned", "DataCloneError");
      if (seen.has(v)) return seen.get(v);
      if (v instanceof Date) return new Date(v.getTime());
      if (v instanceof RegExp) { const r = new RegExp(v.source, v.flags); r.lastIndex = v.lastIndex; return r; }
      if (v instanceof ArrayBuffer) { const c = v.slice(0); seen.set(v, c); return c; }
      if (ArrayBuffer.isView(v)) {
        const buf = clone(v.buffer);
        if (v instanceof DataView) return new DataView(buf, v.byteOffset, v.byteLength);
        return new v.constructor(buf, v.byteOffset, v.length);
      }
      if (v instanceof Blob) return v.slice(0, v.size, v.type);
      if (v instanceof Map) { const m = new Map(); seen.set(v, m); for (const [k, val] of v) m.set(clone(k), clone(val)); return m; }
      if (v instanceof Set) { const s = new Set(); seen.set(v, s); for (const val of v) s.add(clone(val)); return s; }
      if (Array.isArray(v)) { const a = []; seen.set(v, a); for (let i = 0; i < v.length; i++) a[i] = clone(v[i]); return a; }
      if (v instanceof Error) { const Ctor = (typeof globalThis[v.name] === "function") ? globalThis[v.name] : Error; const e = new Ctor(v.message); if (v.stack !== undefined) { try { e.stack = v.stack; } catch (_) {} } return e; }
      const o = {}; seen.set(v, o); for (const k of Object.keys(v)) o[k] = clone(v[k]); return o;
    }
    return clone(value);
  }

  globalThis.Blob = Blob;
  globalThis.structuredClone = structuredClone;
  globalThis.queueMicrotask = function queueMicrotask(callback) {
    if (typeof callback !== "function") throw new TypeError("queueMicrotask: the callback is not a function");
    Promise.resolve().then(function () { try { callback(); } catch (e) { console.error(e); } });
  };
})();
"##;

hyperlight_js_runtime::custom_globals! {
    setup_wintertc,
}

// ── Native CLI entry point (dev/testing only; not used in the guest) ─────────
#[cfg(not(hyperlight))]
fn main() -> anyhow::Result<()> {
    use std::path::Path;
    use std::{env, fs};

    let args: Vec<String> = env::args().collect();
    let file = std::path::PathBuf::from(&args[1]);
    let event = &args[2];

    let handler_script = fs::read_to_string(&file)?;
    let handler_pwd = file.parent().unwrap_or_else(|| Path::new("."));
    env::set_current_dir(handler_pwd)?;

    struct NoOpHost;
    impl hyperlight_js_runtime::host::Host for NoOpHost {
        fn resolve_module(&self, _base: String, name: String) -> anyhow::Result<String> {
            anyhow::bail!("Module '{name}' not found")
        }
        fn load_module(&self, name: String) -> anyhow::Result<String> {
            anyhow::bail!("Module '{name}' not found")
        }
    }

    let mut runtime = hyperlight_js_runtime::JsRuntime::new(NoOpHost)?;
    runtime.register_handler("handler", handler_script, ".")?;
    let result = runtime.run_handler("handler".into(), event.clone(), false)?;
    println!("Handler result: {result}");
    Ok(())
}
