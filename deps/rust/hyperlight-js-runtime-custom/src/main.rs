/*
A custom hyperlight-js guest runtime that bakes a WinterTC ("Minimum Common API") global surface
into QuickJS via the `custom_globals!` extension mechanism. Built out of band (`cargo hyperlight
build`) into the prebuilt blob workerd embeds (//deps/rust/hyperlight-js-runtime:jsruntime.bin).

Two kinds of globals are installed:
  - Rust classes (via #[rquickjs::class]) for byte-level APIs: TextEncoder / TextDecoder.
  - JS polyfills (via ctx.eval) for the rest: atob / btoa.

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
