// Production code must not panic; test code is exempt via clippy.toml allow-*-in-tests.
#![deny(clippy::expect_used, clippy::panic, clippy::unreachable)]
#![deny(clippy::todo, clippy::unimplemented)]

use base64::Engine as _;
use base64::engine::DecodePaddingMode;
use base64::engine::GeneralPurpose;
use base64::engine::GeneralPurposeConfig;
use base64::engine::general_purpose::STANDARD;
use serde::Deserialize;
use serde::Serialize;

const MAX_ENVELOPE_BYTES: usize = 60 * 1024;
const MAX_BODY_BYTES: usize = 32 * 1024;
const MAX_HEADERS: usize = 64;
const MAX_HEADER_BYTES: usize = 8 * 1024;
const MAX_URL_BYTES: usize = 8 * 1024;
const MAX_METHOD_BYTES: usize = 32;
const MAX_REQUEST_ID_BYTES: usize = 64;
const MAX_WORKER_VERSION_BYTES: usize = 256;
const MAX_COMPATIBILITY_FLAGS: usize = 32;
const MAX_COMPATIBILITY_FLAG_BYTES: usize = 64;
const MAX_COMPATIBILITY_FLAGS_BYTES: usize = 2 * 1024;
const MAX_MODULES: usize = 32;
const MAX_MODULE_NAME_BYTES: usize = 256;
const MAX_MODULE_SOURCE_BYTES: usize = 48 * 1024;
const MAX_WASM_MODULE_BYTES: usize = 48 * 1024;
const MAX_MODULE_SOURCES_BYTES: usize = 48 * 1024;

const BASE64_INDIFFERENT_PADDING: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

type Result<T> = std::result::Result<T, ProtocolError>;

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
struct ProtocolError {
    message: String,
}

impl ProtocolError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[cxx::bridge(namespace = "workerd::rust::sandbox_executor")]
mod ffi {
    enum Method {
        Get,
        Head,
        Post,
        Put,
        Delete,
        Patch,
        Options,
        Trace,
        Purge,
    }

    enum ModuleType {
        EsModule,
        Text,
        Json,
        Wasm,
    }

    struct Header {
        name: String,
        value: String,
    }

    struct Module {
        name: String,
        module_type: ModuleType,
        source: Vec<u8>,
    }

    struct InitBundle {
        worker_version: String,
        compatibility_date: String,
        compatibility_flags: Vec<String>,
        main_module: String,
        modules: Vec<Module>,
    }

    struct Request {
        request_id: String,
        method: Method,
        url: String,
        headers: Vec<Header>,
        body: Vec<u8>,
    }

    struct Response {
        status_code: u16,
        headers: Vec<Header>,
        body: Vec<u8>,
    }

    extern "Rust" {
        type ProtocolState;

        fn new_protocol_state() -> Box<ProtocolState>;
        fn initialize(self: &mut ProtocolState, input: &[u8]) -> Result<InitBundle>;
        fn begin_request(self: &mut ProtocolState, input: &[u8]) -> Result<Request>;
        fn complete_response(
            self: &mut ProtocolState,
            request_id: &str,
            response: Response,
        ) -> Result<Vec<u8>>;
        fn complete_failure(
            self: &mut ProtocolState,
            request_id: &str,
            description: &str,
        ) -> Result<Vec<u8>>;

        fn serialize_init(bundle: InitBundle) -> Result<Vec<u8>>;
        fn serialize_response(request_id: &str, response: Response) -> Result<Vec<u8>>;
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct InitEnvelope {
    protocol_version: u8,
    worker_version: String,
    compatibility_date: String,
    compatibility_flags: Vec<String>,
    main_module: String,
    modules: Vec<ModuleEnvelope>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ModuleEnvelope {
    name: String,
    #[serde(rename = "type")]
    module_type: ModuleTypeEnvelope,
    source: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum ModuleTypeEnvelope {
    #[serde(rename = "esModule")]
    EsModule,
    #[serde(rename = "text")]
    Text,
    #[serde(rename = "json")]
    Json,
    #[serde(rename = "wasm")]
    Wasm,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestEnvelope {
    protocol_version: u8,
    request_id: String,
    method: String,
    url: String,
    headers: Vec<HeaderEnvelope>,
    body_base64: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct HeaderEnvelope {
    name: String,
    value: String,
}

#[derive(Debug, Serialize)]
struct ResponseEnvelope<'a> {
    protocol_version: u8,
    request_id: &'a str,
    status: u16,
    headers: Vec<HeaderEnvelope>,
    body_base64: String,
}

struct ProtocolState {
    initialized: bool,
    active_request_id: Option<String>,
}

fn new_protocol_state() -> Box<ProtocolState> {
    Box::new(ProtocolState {
        initialized: false,
        active_request_id: None,
    })
}

impl ProtocolState {
    fn initialize(&mut self, input: &[u8]) -> Result<ffi::InitBundle> {
        if self.initialized {
            return Err(ProtocolError::new("executor already initialized"));
        }
        let bundle = parse_init(input)?;
        self.initialized = true;
        Ok(bundle)
    }

    fn begin_request(&mut self, input: &[u8]) -> Result<ffi::Request> {
        if !self.initialized {
            return Err(ProtocolError::new("executor is not initialized"));
        }
        if self.active_request_id.is_some() {
            return Err(ProtocolError::new("executor request already active"));
        }
        let request = parse_request(input)?;
        self.active_request_id = Some(request.request_id.clone());
        Ok(request)
    }

    fn complete_response(&mut self, request_id: &str, response: ffi::Response) -> Result<Vec<u8>> {
        let serialized = serialize_response(request_id, response)?;
        self.finish_request(request_id)?;
        Ok(serialized)
    }

    fn complete_failure(&mut self, request_id: &str, description: &str) -> Result<Vec<u8>> {
        let body = serde_json::to_vec(&serde_json::json!({
            "error": "worker_execution_failed",
            "exception": description,
        }))
        .map_err(|error| {
            ProtocolError::new(format!("failed to serialize worker error: {error}"))
        })?;
        let serialized = serialize_response(
            request_id,
            ffi::Response {
                status_code: 502,
                headers: vec![ffi::Header {
                    name: "content-type".to_owned(),
                    value: "application/json".to_owned(),
                }],
                body,
            },
        )?;
        self.finish_request(request_id)?;
        Ok(serialized)
    }

    fn finish_request(&mut self, request_id: &str) -> Result<()> {
        let Some(active_request_id) = self.active_request_id.take() else {
            return Err(ProtocolError::new("executor has no active request"));
        };
        if active_request_id != request_id {
            self.active_request_id = Some(active_request_id);
            return Err(ProtocolError::new("executor request ID mismatch"));
        }
        Ok(())
    }
}

fn parse_init(input: &[u8]) -> Result<ffi::InitBundle> {
    if input.len() > MAX_ENVELOPE_BYTES {
        return Err(ProtocolError::new("init envelope exceeds limit"));
    }
    let envelope: InitEnvelope = serde_json::from_slice(input)
        .map_err(|error| ProtocolError::new(format!("invalid init envelope: {error}")))?;
    validate_init(&envelope)?;
    let canonical = serde_json::to_vec(&envelope).map_err(|error| {
        ProtocolError::new(format!("failed to serialize init envelope: {error}"))
    })?;
    if canonical != input {
        return Err(ProtocolError::new("init envelope is not canonical JSON"));
    }
    init_to_ffi(envelope)
}

fn validate_init(envelope: &InitEnvelope) -> Result<()> {
    if envelope.protocol_version != 1 {
        return Err(ProtocolError::new("invalid init protocol version"));
    }
    if !valid_identifier(&envelope.worker_version, MAX_WORKER_VERSION_BYTES) {
        return Err(ProtocolError::new("invalid Worker version"));
    }
    if !valid_compatibility_date(&envelope.compatibility_date) {
        return Err(ProtocolError::new("invalid compatibility date"));
    }
    if envelope.compatibility_flags.len() > MAX_COMPATIBILITY_FLAGS {
        return Err(ProtocolError::new("too many compatibility flags"));
    }
    let mut aggregate_flag_bytes = 0usize;
    let mut previous_flag: Option<&str> = None;
    for flag in &envelope.compatibility_flags {
        if !valid_compatibility_flag(flag) {
            return Err(ProtocolError::new("invalid compatibility flag"));
        }
        aggregate_flag_bytes = aggregate_flag_bytes
            .checked_add(flag.len())
            .ok_or_else(|| ProtocolError::new("compatibility flags exceed size limit"))?;
        if aggregate_flag_bytes > MAX_COMPATIBILITY_FLAGS_BYTES {
            return Err(ProtocolError::new("compatibility flags exceed size limit"));
        }
        if previous_flag.is_some_and(|previous| previous >= flag.as_str()) {
            return Err(ProtocolError::new(
                "compatibility flags must be sorted and unique",
            ));
        }
        previous_flag = Some(flag);
    }
    if !valid_module_name(&envelope.main_module) {
        return Err(ProtocolError::new("invalid main module"));
    }
    if envelope.modules.is_empty() || envelope.modules.len() > MAX_MODULES {
        return Err(ProtocolError::new("invalid module count"));
    }

    let mut aggregate_source_bytes = 0usize;
    let mut previous_module: Option<&str> = None;
    for (index, module) in envelope.modules.iter().enumerate() {
        if !valid_module_name(&module.name) {
            return Err(ProtocolError::new("invalid module name"));
        }
        let source_bytes = match module.module_type {
            ModuleTypeEnvelope::Wasm => decode_base64_limited(
                &module.source,
                MAX_WASM_MODULE_BYTES,
                "invalid Wasm module source",
                "Wasm module exceeds decoded size limit",
            )?
            .len(),
            ModuleTypeEnvelope::EsModule | ModuleTypeEnvelope::Text | ModuleTypeEnvelope::Json => {
                if module.source.len() > MAX_MODULE_SOURCE_BYTES {
                    return Err(ProtocolError::new("module source exceeds size limit"));
                }
                module.source.len()
            }
        };
        aggregate_source_bytes = aggregate_source_bytes
            .checked_add(source_bytes)
            .ok_or_else(|| ProtocolError::new("module sources exceed size limit"))?;
        if aggregate_source_bytes > MAX_MODULE_SOURCES_BYTES {
            return Err(ProtocolError::new("module sources exceed size limit"));
        }
        if index == 0 {
            if module.name != envelope.main_module
                || !matches!(module.module_type, ModuleTypeEnvelope::EsModule)
            {
                return Err(ProtocolError::new(
                    "main module must be the first module and an ES module",
                ));
            }
        } else {
            if module.name == envelope.main_module {
                return Err(ProtocolError::new("duplicate main module"));
            }
            if previous_module.is_some_and(|previous| previous >= module.name.as_str()) {
                return Err(ProtocolError::new(
                    "non-main modules must be sorted and unique",
                ));
            }
            previous_module = Some(&module.name);
        }
    }
    Ok(())
}

fn init_to_ffi(envelope: InitEnvelope) -> Result<ffi::InitBundle> {
    let modules = envelope
        .modules
        .into_iter()
        .map(|module| {
            let (module_type, source) = match module.module_type {
                ModuleTypeEnvelope::EsModule => {
                    (ffi::ModuleType::EsModule, module.source.into_bytes())
                }
                ModuleTypeEnvelope::Text => (ffi::ModuleType::Text, module.source.into_bytes()),
                ModuleTypeEnvelope::Json => (ffi::ModuleType::Json, module.source.into_bytes()),
                ModuleTypeEnvelope::Wasm => (
                    ffi::ModuleType::Wasm,
                    decode_base64_limited(
                        &module.source,
                        MAX_WASM_MODULE_BYTES,
                        "invalid Wasm module source",
                        "Wasm module exceeds decoded size limit",
                    )?,
                ),
            };
            Ok(ffi::Module {
                name: module.name,
                module_type,
                source,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ffi::InitBundle {
        worker_version: envelope.worker_version,
        compatibility_date: envelope.compatibility_date,
        compatibility_flags: envelope.compatibility_flags,
        main_module: envelope.main_module,
        modules,
    })
}

fn serialize_init(bundle: ffi::InitBundle) -> Result<Vec<u8>> {
    let envelope = InitEnvelope {
        protocol_version: 1,
        worker_version: bundle.worker_version,
        compatibility_date: bundle.compatibility_date,
        compatibility_flags: bundle.compatibility_flags,
        main_module: bundle.main_module,
        modules: bundle
            .modules
            .into_iter()
            .map(|module| -> Result<ModuleEnvelope> {
                let module_type = match module.module_type {
                    ffi::ModuleType::EsModule => ModuleTypeEnvelope::EsModule,
                    ffi::ModuleType::Text => ModuleTypeEnvelope::Text,
                    ffi::ModuleType::Json => ModuleTypeEnvelope::Json,
                    ffi::ModuleType::Wasm => ModuleTypeEnvelope::Wasm,
                    _ => return Err(ProtocolError::new("invalid module type")),
                };
                let source = if matches!(module_type, ModuleTypeEnvelope::Wasm) {
                    STANDARD.encode(module.source)
                } else {
                    String::from_utf8(module.source)
                        .map_err(|_| ProtocolError::new("text module source is not UTF-8"))?
                };
                Ok(ModuleEnvelope {
                    name: module.name,
                    module_type,
                    source,
                })
            })
            .collect::<Result<Vec<_>>>()?,
    };
    validate_init(&envelope)?;
    let serialized = serde_json::to_vec(&envelope).map_err(|error| {
        ProtocolError::new(format!("failed to serialize init envelope: {error}"))
    })?;
    if serialized.len() > MAX_ENVELOPE_BYTES {
        return Err(ProtocolError::new("init envelope exceeds limit"));
    }
    Ok(serialized)
}

fn parse_request(input: &[u8]) -> Result<ffi::Request> {
    if input.len() > MAX_ENVELOPE_BYTES {
        return Err(ProtocolError::new("request envelope exceeds limit"));
    }
    let envelope: RequestEnvelope = serde_json::from_slice(input)
        .map_err(|error| ProtocolError::new(format!("invalid request envelope: {error}")))?;
    if envelope.protocol_version != 1 {
        return Err(ProtocolError::new("invalid protocol version"));
    }
    if !valid_identifier(&envelope.request_id, MAX_REQUEST_ID_BYTES) {
        return Err(ProtocolError::new("invalid request ID"));
    }
    let method = parse_method(&envelope.method)?;
    if envelope.url.len() > MAX_URL_BYTES
        || !(envelope.url.starts_with("http://") || envelope.url.starts_with("https://"))
        || envelope
            .url
            .bytes()
            .any(|byte| byte <= 0x20 || byte == 0x7f)
    {
        return Err(ProtocolError::new("invalid URL"));
    }
    let headers = validate_headers(envelope.headers)?;
    let body = decode_base64_limited(
        &envelope.body_base64,
        MAX_BODY_BYTES,
        "invalid body",
        "body exceeds size limit",
    )?;
    Ok(ffi::Request {
        request_id: envelope.request_id,
        method,
        url: envelope.url,
        headers,
        body,
    })
}

fn parse_method(method: &str) -> Result<ffi::Method> {
    if method.is_empty()
        || method.len() > MAX_METHOD_BYTES
        || method
            .bytes()
            .any(|byte| !byte.is_ascii_uppercase() && byte != b'-')
    {
        return Err(ProtocolError::new("invalid method"));
    }
    match method {
        "GET" => Ok(ffi::Method::Get),
        "HEAD" => Ok(ffi::Method::Head),
        "POST" => Ok(ffi::Method::Post),
        "PUT" => Ok(ffi::Method::Put),
        "DELETE" => Ok(ffi::Method::Delete),
        "PATCH" => Ok(ffi::Method::Patch),
        "OPTIONS" => Ok(ffi::Method::Options),
        "TRACE" => Ok(ffi::Method::Trace),
        "PURGE" => Ok(ffi::Method::Purge),
        _ => Err(ProtocolError::new("unsupported method")),
    }
}

fn validate_headers(headers: Vec<HeaderEnvelope>) -> Result<Vec<ffi::Header>> {
    if headers.len() > MAX_HEADERS {
        return Err(ProtocolError::new("too many headers"));
    }
    let mut aggregate_size = 0usize;
    headers
        .into_iter()
        .map(|header| {
            if !valid_header_name(&header.name) || !valid_header_value(&header.value) {
                return Err(ProtocolError::new("invalid header"));
            }
            aggregate_size = aggregate_size
                .checked_add(header.name.len() + header.value.len())
                .ok_or_else(|| ProtocolError::new("headers exceed size limit"))?;
            if aggregate_size > MAX_HEADER_BYTES {
                return Err(ProtocolError::new("headers exceed size limit"));
            }
            Ok(ffi::Header {
                name: header.name,
                value: header.value,
            })
        })
        .collect()
}

fn serialize_response(request_id: &str, response: ffi::Response) -> Result<Vec<u8>> {
    if !valid_identifier(request_id, MAX_REQUEST_ID_BYTES) {
        return Err(ProtocolError::new("invalid response request ID"));
    }
    if !(100..=599).contains(&response.status_code) {
        return Err(ProtocolError::new("invalid response status"));
    }
    if response.body.len() > MAX_BODY_BYTES {
        return Err(ProtocolError::new("response body exceeds size limit"));
    }
    let headers = response
        .headers
        .into_iter()
        .map(|header| HeaderEnvelope {
            name: header.name,
            value: header.value,
        })
        .collect();
    let headers = validate_response_headers(headers)?;
    let envelope = ResponseEnvelope {
        protocol_version: 1,
        request_id,
        status: response.status_code,
        headers,
        body_base64: STANDARD.encode(response.body),
    };
    let mut serialized = serde_json::to_vec(&envelope).map_err(|error| {
        ProtocolError::new(format!("failed to serialize response envelope: {error}"))
    })?;
    serialized.push(b'\n');
    if serialized.len() > MAX_ENVELOPE_BYTES + 1 {
        return Err(ProtocolError::new("response envelope exceeds size limit"));
    }
    Ok(serialized)
}

fn validate_response_headers(headers: Vec<HeaderEnvelope>) -> Result<Vec<HeaderEnvelope>> {
    if headers.len() > MAX_HEADERS {
        return Err(ProtocolError::new("too many response headers"));
    }
    let mut aggregate_size = 0usize;
    for header in &headers {
        if !valid_header_name(&header.name) || !valid_header_value(&header.value) {
            return Err(ProtocolError::new("invalid response header"));
        }
        aggregate_size = aggregate_size
            .checked_add(header.name.len() + header.value.len())
            .ok_or_else(|| ProtocolError::new("response headers exceed size limit"))?;
        if aggregate_size > MAX_HEADER_BYTES {
            return Err(ProtocolError::new("response headers exceed size limit"));
        }
    }
    Ok(headers)
}

fn decode_base64_limited(
    encoded: &str,
    decoded_limit: usize,
    invalid_message: &str,
    oversized_message: &str,
) -> Result<Vec<u8>> {
    let encoded_limit = decoded_limit.div_ceil(3) * 4;
    if encoded.len() > encoded_limit {
        return Err(ProtocolError::new(oversized_message));
    }
    let decoded = BASE64_INDIFFERENT_PADDING
        .decode(encoded)
        .map_err(|_| ProtocolError::new(invalid_message))?;
    if decoded.len() > decoded_limit {
        return Err(ProtocolError::new(oversized_message));
    }
    Ok(decoded)
}

fn valid_identifier(value: &str, limit: usize) -> bool {
    !value.is_empty()
        && value.len() <= limit
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn valid_compatibility_flag(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_COMPATIBILITY_FLAG_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn valid_compatibility_date(value: &str) -> bool {
    if value.len() != 10 {
        return false;
    }
    let bytes = value.as_bytes();
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes
            .iter()
            .enumerate()
            .any(|(index, byte)| index != 4 && index != 7 && !byte.is_ascii_digit())
    {
        return false;
    }
    let year = value[0..4].parse::<u16>();
    let month = value[5..7].parse::<u8>();
    let day = value[8..10].parse::<u8>();
    matches!(year, Ok(2000..=2999)) && matches!(month, Ok(1..=12)) && matches!(day, Ok(1..=31))
}

fn valid_module_name(value: &str) -> bool {
    if value.is_empty()
        || value.len() > MAX_MODULE_NAME_BYTES
        || value.starts_with('/')
        || value.ends_with('/')
    {
        return false;
    }
    value.split('/').all(|segment| {
        !segment.is_empty()
            && segment != "."
            && segment != ".."
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    })
}

fn valid_header_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 256
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
}

fn valid_header_value(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte == b'\t' || (byte >= 0x20 && byte != 0x7f))
}

#[cfg(test)]
mod tests {
    use super::*;

    const CANONICAL_INIT: &[u8] = br#"{"protocol_version":1,"worker_version":"worker-v1","compatibility_date":"2025-12-31","compatibility_flags":["nodejs_compat"],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"},{"name":"module.wasm","type":"wasm","source":"AGFzbQEAAAA="}]}"#;
    const CANONICAL_REQUEST: &[u8] = br#"{"protocol_version":1,"request_id":"request-1","method":"POST","url":"https://example.test/path","headers":[{"name":"x-test","value":"yes"}],"body_base64":"aGVsbG8="}"#;

    #[test]
    fn canonical_init_round_trips_with_wasm() {
        let bundle = parse_init(CANONICAL_INIT).expect("canonical init should parse");
        assert_eq!(
            serialize_init(bundle).expect("init should serialize"),
            CANONICAL_INIT
        );
    }

    #[test]
    fn canonical_request_parses() {
        let request = parse_request(CANONICAL_REQUEST).expect("canonical request should parse");
        assert_eq!(
            (
                request.request_id,
                request.url,
                request.headers[0].name.clone(),
                request.headers[0].value.clone(),
                request.body,
            ),
            (
                "request-1".to_owned(),
                "https://example.test/path".to_owned(),
                "x-test".to_owned(),
                "yes".to_owned(),
                b"hello".to_vec(),
            )
        );
        assert!(matches!(request.method, ffi::Method::Post));
    }

    #[test]
    fn canonical_response_is_stable() {
        let response = serialize_response(
            "request-1",
            ffi::Response {
                status_code: 201,
                headers: vec![ffi::Header {
                    name: "content-type".to_owned(),
                    value: "text/plain".to_owned(),
                }],
                body: b"hello".to_vec(),
            },
        )
        .expect("response should serialize");
        assert_eq!(
            response,
            br#"{"protocol_version":1,"request_id":"request-1","status":201,"headers":[{"name":"content-type","value":"text/plain"}],"body_base64":"aGVsbG8="}
"#
        );
    }

    #[test]
    fn rejects_noncanonical_and_duplicate_init_fields() {
        let noncanonical = br#"{ "protocol_version":1,"worker_version":"worker-v1","compatibility_date":"2025-12-31","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}]}"#;
        let duplicate = br#"{"protocol_version":1,"protocol_version":1,"worker_version":"worker-v1","compatibility_date":"2025-12-31","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"}]}"#;
        assert!(parse_init(noncanonical).is_err());
        assert!(parse_init(duplicate).is_err());
    }

    #[test]
    fn rejects_duplicate_request_fields() {
        let duplicate = br#"{"protocol_version":1,"request_id":"one","request_id":"two","method":"GET","url":"https://example.test/","headers":[],"body_base64":""}"#;
        assert!(parse_request(duplicate).is_err());
    }

    #[test]
    fn rejects_oversized_envelopes_and_sources() {
        assert!(parse_init(&vec![b'x'; MAX_ENVELOPE_BYTES + 1]).is_err());
        assert!(parse_request(&vec![b'x'; MAX_ENVELOPE_BYTES + 1]).is_err());
        let encoded = STANDARD.encode(vec![0u8; MAX_WASM_MODULE_BYTES + 1]);
        assert!(
            decode_base64_limited(&encoded, MAX_WASM_MODULE_BYTES, "invalid", "oversized").is_err()
        );
    }

    #[test]
    fn accepts_component_adapter_sized_module() {
        const COMPONENT_ADAPTER_BYTES: usize = 45_194;
        let bundle = ffi::InitBundle {
            worker_version: "worker-v1".to_owned(),
            compatibility_date: "2025-12-31".to_owned(),
            compatibility_flags: Vec::new(),
            main_module: "worker.js".to_owned(),
            modules: vec![ffi::Module {
                name: "worker.js".to_owned(),
                module_type: ffi::ModuleType::EsModule,
                source: vec![b' '; COMPONENT_ADAPTER_BYTES],
            }],
        };

        assert!(serialize_init(bundle).is_ok());
    }

    #[test]
    fn accepts_maximum_text_module_and_rejects_one_byte_more() {
        let bundle = |source_size| ffi::InitBundle {
            worker_version: "worker-v1".to_owned(),
            compatibility_date: "2025-12-31".to_owned(),
            compatibility_flags: Vec::new(),
            main_module: "worker.js".to_owned(),
            modules: vec![ffi::Module {
                name: "worker.js".to_owned(),
                module_type: ffi::ModuleType::EsModule,
                source: vec![b' '; source_size],
            }],
        };

        assert!(serialize_init(bundle(MAX_MODULE_SOURCE_BYTES)).is_ok());
        assert!(serialize_init(bundle(MAX_MODULE_SOURCE_BYTES + 1)).is_err());
    }

    #[test]
    fn rejects_invalid_module_order_and_aggregate_size() {
        let duplicate = br#"{"protocol_version":1,"worker_version":"worker-v1","compatibility_date":"2025-12-31","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default {}"},{"name":"a.js","type":"text","source":"a"},{"name":"a.js","type":"text","source":"b"}]}"#;
        assert!(parse_init(duplicate).is_err());

        let bundle = ffi::InitBundle {
            worker_version: "worker-v1".to_owned(),
            compatibility_date: "2025-12-31".to_owned(),
            compatibility_flags: Vec::new(),
            main_module: "worker.js".to_owned(),
            modules: vec![
                ffi::Module {
                    name: "worker.js".to_owned(),
                    module_type: ffi::ModuleType::EsModule,
                    source: vec![b'a'; MAX_MODULE_SOURCE_BYTES],
                },
                ffi::Module {
                    name: "z.js".to_owned(),
                    module_type: ffi::ModuleType::Text,
                    source: vec![b'b'; MAX_MODULE_SOURCES_BYTES - MAX_MODULE_SOURCE_BYTES + 1],
                },
            ],
        };
        assert!(serialize_init(bundle).is_err());
    }

    #[test]
    fn wasm_source_decodes_to_binary_bytes() {
        let bundle = parse_init(CANONICAL_INIT).expect("canonical init should parse");
        assert_eq!(
            bundle.modules[1].source,
            vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]
        );
    }

    #[test]
    fn state_transitions_are_enforced() {
        let mut state = new_protocol_state();
        assert!(state.begin_request(CANONICAL_REQUEST).is_err());
        state
            .initialize(CANONICAL_INIT)
            .expect("initialization should succeed");
        assert!(state.initialize(CANONICAL_INIT).is_err());
        let request = state
            .begin_request(CANONICAL_REQUEST)
            .expect("request should begin");
        assert!(state.begin_request(CANONICAL_REQUEST).is_err());
        assert!(
            state
                .complete_response(
                    "wrong-request",
                    ffi::Response {
                        status_code: 200,
                        headers: Vec::new(),
                        body: Vec::new(),
                    }
                )
                .is_err()
        );
        state
            .complete_response(
                &request.request_id,
                ffi::Response {
                    status_code: 200,
                    headers: Vec::new(),
                    body: Vec::new(),
                },
            )
            .expect("request should complete");
        state
            .begin_request(CANONICAL_REQUEST)
            .expect("next request should begin");
    }
}
