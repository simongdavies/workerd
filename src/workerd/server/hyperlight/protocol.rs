// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use bytes::Bytes;
use hyperlight_common::flatbuffer_wrappers::ExternalValueSource;
use hyperlight_common::flatbuffer_wrappers::function_call::FunctionCall;
use hyperlight_common::flatbuffer_wrappers::function_call::FunctionCallType;
use hyperlight_common::flatbuffer_wrappers::function_types::ParameterValue;
use hyperlight_common::flatbuffer_wrappers::function_types::ReturnType;

/// A validated operation. `ServeArtifact` owns a terminal long-lived call; the trailing
/// environment version belongs to the kernel.
pub enum Call {
    Configure {
        config: Vec<u8>,
        socket: String,
        reply_limit: usize,
    },
    ConfigureArtifact {
        length: usize,
        budget: usize,
        digest: [u8; 32],
        socket: String,
        reply_limit: usize,
    },
    ServeArtifact {
        length: usize,
        budget: usize,
        digest: [u8; 32],
        socket: String,
    },
    Fetch(Vec<u8>),
}

struct External<'a>(&'a [u8]);

pub(crate) fn validate_artifact_bounds(length: usize, budget: usize) -> Result<()> {
    if length == 0
        || !length.is_multiple_of(8)
        || length > budget
        || isize::try_from(budget).is_err()
    {
        bail!("invalid admitted config length or read budget");
    }
    Ok(())
}

/// Verified artifact fields for the native process entry, not a decoded guest-call variant.
#[derive(Debug, PartialEq, Eq)]
pub struct ArtifactAdmission {
    pub length: usize,
    pub budget: usize,
    pub digest: [u8; 32],
    pub socket: String,
}

/// Native PID1 entry using the existing artifact admission, without a new guest-call ABI.
/// The embedding host must bind these pins to its measured image; they are not caller identity.
pub fn decode_serve_args(args: &[String]) -> Result<ArtifactAdmission> {
    let [operation, length, budget, digest, socket] = args else {
        bail!("usage: --serve-artifact <config-bytes> <read-budget> <sha256> <socket-name>");
    };
    if operation != "--serve-artifact" {
        bail!("unsupported selected executor entry operation");
    }
    let length = length.parse::<usize>()?;
    let budget = budget.parse::<usize>()?;
    validate_artifact_bounds(length, budget)?;
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("artifact SHA256 must contain exactly 64 hexadecimal digits");
    }
    let mut sha256 = [0; 32];
    for (index, byte) in sha256.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&digest[index * 2..index * 2 + 2], 16)?;
    }
    if socket.is_empty() || socket.as_bytes().contains(&0) {
        bail!("selected ingress socket name must be nonempty and contain no NUL");
    }
    Ok(ArtifactAdmission {
        length,
        budget,
        digest: sha256,
        socket: socket.clone(),
    })
}

fn reply_limit(value: u64) -> Result<usize> {
    let limit = usize::try_from(value)?;
    if limit == 0
        || limit
            .checked_add(size_of::<i32>())
            .is_none_or(|size| isize::try_from(size).is_err())
    {
        bail!("invalid host-supplied reply limit");
    }
    Ok(limit)
}

impl ExternalValueSource for External<'_> {
    fn take_bytes(&mut self, length: usize) -> Result<Vec<u8>> {
        if length > self.0.len() {
            bail!("truncated Hyperlight external value");
        }
        let (value, rest) = self.0.split_at(length);
        self.0 = rest;
        Ok(value.to_vec())
    }

    fn take_chunks(&mut self, _length: usize) -> Result<Vec<Bytes>> {
        bail!("finite executor calls require contiguous VecBytes");
    }

    fn finish(&mut self) -> Result<()> {
        if !self.0.is_empty() {
            bail!("unused bytes after Hyperlight external values");
        }
        Ok(())
    }
}

/// Decodes only explicitly registered one-String calls; standard operations remain unchanged.
pub fn decode_string_call(
    frame: &[u8],
    capacity: usize,
    names: &[&str],
) -> Result<Option<(String, String)>> {
    if frame.len() > capacity || frame.len() < 4 {
        bail!("invalid Hyperlight call length");
    }
    let length = usize::try_from(u32::from_le_bytes(frame[..4].try_into()?))?
        .checked_add(4)
        .filter(|&length| length <= frame.len())
        .context("truncated Hyperlight control buffer")?;
    let (control, external) = frame.split_at(length);
    let call = FunctionCall::decode(control, &mut External(external))?;
    if !names.contains(&call.function_name.as_str()) {
        return Ok(None);
    }
    if call.function_call_type() != FunctionCallType::Guest
        || call.expected_return_type != ReturnType::Void
    {
        bail!("registered call expects a guest call with a void SDK return");
    }
    let parameters = call
        .parameters
        .context("missing registered call argument")?;
    if parameters.len() != 1 || !external.is_empty() {
        bail!("registered call requires exactly one String and no external bytes");
    }
    let Some(ParameterValue::String(value)) = parameters.into_iter().next() else {
        bail!("registered call requires a String");
    };
    if value
        .len()
        .checked_add(call.function_name.len())
        .is_none_or(|n| n > frame.len())
    {
        bail!("registered call owned values exceed input budget");
    }
    Ok(Some((call.function_name, value)))
}

pub fn decode(frame: &[u8], capacity: usize) -> Result<Call> {
    if frame.len() > capacity || frame.len() < 4 {
        bail!("invalid Hyperlight call length");
    }
    let prefix: [u8; 4] = frame[..4].try_into()?;
    let control_length = usize::try_from(u32::from_le_bytes(prefix))?
        .checked_add(4)
        .filter(|&length| length <= frame.len())
        .context("truncated Hyperlight control buffer")?;
    let (control, external) = frame.split_at(control_length);
    let call = FunctionCall::decode(control, &mut External(external))?;
    if call.function_call_type() != FunctionCallType::Guest
        || call.expected_return_type != ReturnType::Void
    {
        bail!("executor expects a guest call with a void SDK return");
    }
    let parameters = call.parameters.unwrap_or_default();
    match (call.function_name.as_str(), parameters.as_slice()) {
        (
            "configure",
            [
                ParameterValue::VecBytes(_),
                ParameterValue::String(_),
                ParameterValue::ULong(limit),
                ParameterValue::ULong(_),
            ],
        ) => {
            let reply_limit = reply_limit(*limit)?;
            let mut parameters = parameters.into_iter();
            let Some(ParameterValue::VecBytes(config)) = parameters.next() else {
                unreachable!()
            };
            let Some(ParameterValue::String(socket)) = parameters.next() else {
                unreachable!()
            };
            Ok(Call::Configure {
                config,
                socket,
                reply_limit,
            })
        }
        (
            "configure_artifact",
            [
                ParameterValue::ULong(length),
                ParameterValue::ULong(budget),
                ParameterValue::VecBytes(digest),
                ParameterValue::String(_),
                ParameterValue::ULong(limit),
                ParameterValue::ULong(_),
            ],
        ) => {
            let length = usize::try_from(*length)?;
            let budget = usize::try_from(*budget)?;
            validate_artifact_bounds(length, budget)?;
            let digest: [u8; 32] = digest.as_slice().try_into()?;
            let reply_limit = reply_limit(*limit)?;
            let Some(ParameterValue::String(socket)) = parameters.into_iter().nth(3) else {
                unreachable!()
            };
            Ok(Call::ConfigureArtifact {
                length,
                budget,
                digest,
                socket,
                reply_limit,
            })
        }
        ("fetch", [ParameterValue::VecBytes(_), ParameterValue::ULong(_)]) => {
            let Some(ParameterValue::VecBytes(request)) = parameters.into_iter().next() else {
                unreachable!()
            };
            Ok(Call::Fetch(request))
        }
        (
            "serve_artifact",
            [
                ParameterValue::ULong(length),
                ParameterValue::ULong(budget),
                ParameterValue::VecBytes(digest),
                ParameterValue::String(_),
                ParameterValue::ULong(_),
            ],
        ) => {
            let length = usize::try_from(*length)?;
            let budget = usize::try_from(*budget)?;
            validate_artifact_bounds(length, budget)?;
            let digest = digest.as_slice().try_into()?;
            let Some(ParameterValue::String(socket)) = parameters.into_iter().nth(3) else {
                unreachable!()
            };
            Ok(Call::ServeArtifact {
                length,
                budget,
                digest,
                socket,
            })
        }
        _ => bail!("unknown executor call or invalid arguments"),
    }
}

#[cfg(test)]
#[path = "protocol-test.rs"]
mod tests;
