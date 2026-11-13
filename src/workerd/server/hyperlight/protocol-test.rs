use flatbuffers::FlatBufferBuilder;
use hyperlight_common::flatbuffer_wrappers::ExternalValueSink;

use super::*;

#[test]
fn native_selected_bootstrap_preserves_existing_artifact_admission_fields() {
    let args = [
        "--serve-artifact",
        "64",
        "128",
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        "rpc",
    ]
    .map(str::to_owned);
    assert_eq!(
        decode_serve_args(&args).unwrap(),
        ArtifactAdmission {
            length: 64,
            budget: 128,
            digest: std::array::from_fn(|i| i as u8),
            socket: "rpc".to_owned(),
        }
    );
}

#[test]
fn native_selected_bootstrap_refuses_invalid_or_extra_admission_arguments() {
    let valid = [
        "--serve-artifact",
        "64",
        "128",
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        "rpc",
    ]
    .map(str::to_owned);
    for (index, value) in [
        (0, "fetch"),
        (1, "0"),
        (1, "65"),
        (1, "129"),
        (2, "0"),
        (3, "bad"),
        (
            3,
            "_00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        ),
        (4, ""),
        (4, "rpc\0"),
    ] {
        let mut args = valid.clone();
        args[index] = value.to_owned();
        assert!(decode_serve_args(&args).is_err(), "{index}: {value:?}");
    }
    assert!(decode_serve_args(&valid[..4]).is_err());
    let mut extra = valid.to_vec();
    extra.push("unexpected".to_owned());
    assert!(decode_serve_args(&extra).is_err());
}

struct Sink(Vec<u8>);

#[test]
fn registered_string_calls_are_opt_in_and_do_not_change_standard_shapes() {
    for name in ["scheduled", "queue"] {
        let bytes = frame(
            name,
            vec![ParameterValue::String("{\"exact\":true}".into())],
        );
        assert!(decode(&bytes, bytes.len()).is_err());
        assert!(
            decode_string_call(&bytes, bytes.len(), &[])
                .unwrap()
                .is_none()
        );
        assert_eq!(
            decode_string_call(&bytes, bytes.len(), &[name]).unwrap(),
            Some((name.to_owned(), "{\"exact\":true}".to_owned())),
        );
        assert!(decode_string_call(&bytes, bytes.len() - 1, &[name]).is_err());
        for end in 0..bytes.len() {
            assert!(decode_string_call(&bytes[..end], bytes.len(), &[name]).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(decode_string_call(&trailing, trailing.len(), &[name]).is_err());
        for values in [
            vec![],
            vec![ParameterValue::ULong(1)],
            vec![ParameterValue::VecBytes(b"{}".to_vec())],
            vec![
                ParameterValue::String("{}".into()),
                ParameterValue::ULong(1),
            ],
        ] {
            let bytes = frame(name, values);
            assert!(decode_string_call(&bytes, bytes.len(), &[name]).is_err());
        }
        for (direction, result) in [
            (FunctionCallType::Host, ReturnType::Void),
            (FunctionCallType::Guest, ReturnType::Int),
        ] {
            let call = FunctionCall::new(
                name.to_owned(),
                Some(vec![ParameterValue::String("{}".into())]),
                direction,
                result,
            );
            let mut builder = FlatBufferBuilder::new();
            let mut external = Sink(Vec::new());
            let bytes = call.encode(&mut builder, &mut external).unwrap();
            assert!(decode_string_call(bytes, bytes.len(), &[name]).is_err());
        }
    }
    let bytes = frame(
        "fetch",
        vec![
            ParameterValue::VecBytes(vec![0, 255]),
            ParameterValue::ULong(7),
        ],
    );
    assert!(
        decode_string_call(&bytes, bytes.len(), &["scheduled", "queue"])
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        decode(&bytes, bytes.len()).unwrap(),
        Call::Fetch(_)
    ));
}

#[test]
fn terminal_serve_uses_five_typed_metadata_arguments_without_a_finite_reply_budget() {
    let values = || {
        vec![
            ParameterValue::ULong(8),
            ParameterValue::ULong(16 * 1024 * 1024),
            ParameterValue::VecBytes(vec![7; 32]),
            ParameterValue::String("ingress".to_owned()),
            ParameterValue::ULong(0),
        ]
    };
    let bytes = frame("serve_artifact", values());
    let Call::ServeArtifact {
        length,
        budget,
        digest,
        socket,
    } = decode(&bytes, bytes.len()).unwrap()
    else {
        panic!("wrong call")
    };
    assert_eq!(length, 8);
    assert_eq!(budget, 16 * 1024 * 1024);
    assert_eq!(digest, [7; 32]);
    assert_eq!(socket, "ingress");
    for index in 0..5 {
        let mut arguments = values();
        arguments[index] = ParameterValue::Bool(true);
        let bytes = frame("serve_artifact", arguments);
        assert!(
            decode(&bytes, bytes.len()).is_err(),
            "accepted wrong type at {index}"
        );
    }
    for (length, budget, digest) in [(0, 8, 32), (8, 7, 32), (7, 8, 32), (8, 8, 31), (8, 8, 33)] {
        let bytes = frame(
            "serve_artifact",
            vec![
                ParameterValue::ULong(length),
                ParameterValue::ULong(budget),
                ParameterValue::VecBytes(vec![0; digest]),
                ParameterValue::String("ingress".to_owned()),
                ParameterValue::ULong(1),
            ],
        );
        assert!(decode(&bytes, bytes.len()).is_err());
    }
    let mut extra = values();
    extra.push(ParameterValue::ULong(1));
    let bytes = frame("serve_artifact", extra);
    assert!(decode(&bytes, bytes.len()).is_err());
}

impl<'a> ExternalValueSink<'a> for Sink {
    fn push_bytes(&mut self, value: &'a [u8]) -> Result<()> {
        self.0.extend_from_slice(value);
        Ok(())
    }

    fn push_chunks(&mut self, value: &'a [Bytes]) -> Result<()> {
        for chunk in value {
            self.0.extend_from_slice(chunk);
        }
        Ok(())
    }
}

fn frame(name: &str, values: Vec<ParameterValue>) -> Vec<u8> {
    let call = FunctionCall::new(
        name.to_owned(),
        Some(values),
        FunctionCallType::Guest,
        ReturnType::Void,
    );
    let mut builder = FlatBufferBuilder::new();
    let mut external = Sink(Vec::new());
    let mut result = call.encode(&mut builder, &mut external).unwrap().to_vec();
    result.extend_from_slice(&external.0);
    result
}

#[test]
fn official_external_bytes_decode_into_the_exact_configure_contract() {
    let bytes = frame(
        "configure",
        vec![
            ParameterValue::VecBytes(vec![0, 255, 0, 128]),
            ParameterValue::String("http".to_owned()),
            ParameterValue::ULong(128),
            ParameterValue::ULong(42),
        ],
    );
    match decode(&bytes, bytes.len()).unwrap() {
        Call::Configure {
            config,
            socket,
            reply_limit,
        } => {
            assert_eq!(config, [0, 255, 0, 128]);
            assert_eq!(socket, "http");
            assert_eq!(reply_limit, 128);
        }
        Call::Fetch(_) | Call::ConfigureArtifact { .. } | Call::ServeArtifact { .. } => {
            panic!("wrong call")
        }
    }
}

#[test]
fn finite_fetch_preserves_binary_body_and_environment_trailer() {
    let bytes = frame(
        "fetch",
        vec![
            ParameterValue::VecBytes(vec![0, 255, 0, 128]),
            ParameterValue::ULong(7),
        ],
    );
    let Call::Fetch(body) = decode(&bytes, bytes.len()).unwrap() else {
        panic!("wrong call")
    };
    assert_eq!(body, [0, 255, 0, 128]);
}

#[test]
fn truncated_control_external_and_unused_tail_are_refused() {
    let bytes = frame(
        "fetch",
        vec![
            ParameterValue::VecBytes(vec![0, 1, 2, 3]),
            ParameterValue::ULong(1),
        ],
    );
    for end in 0..bytes.len() {
        assert!(
            decode(&bytes[..end], bytes.len()).is_err(),
            "accepted prefix {end}"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode(&trailing, trailing.len()).is_err());
    assert!(decode(&bytes, bytes.len() - 1).is_err());
}

#[test]
fn exact_arguments_are_required_and_host_known_output_limit_is_checked() {
    for parameters in [
        vec![],
        vec![
            ParameterValue::String("not binary".to_owned()),
            ParameterValue::ULong(1),
        ],
        vec![ParameterValue::VecBytes(vec![])],
        vec![
            ParameterValue::VecBytes(vec![]),
            ParameterValue::ULong(1),
            ParameterValue::ULong(2),
        ],
    ] {
        let bytes = frame("fetch", parameters);
        assert!(decode(&bytes, bytes.len()).is_err());
    }
    for limit in [0, u64::MAX] {
        let bytes = frame(
            "configure",
            vec![
                ParameterValue::VecBytes(vec![]),
                ParameterValue::String("http".to_owned()),
                ParameterValue::ULong(limit),
                ParameterValue::ULong(1),
            ],
        );
        assert!(decode(&bytes, bytes.len()).is_err());
    }
}

#[test]
fn wrong_direction_return_type_and_chunked_values_are_refused() {
    for (direction, result) in [
        (FunctionCallType::Host, ReturnType::Void),
        (FunctionCallType::Guest, ReturnType::Int),
    ] {
        let call = FunctionCall::new("fetch".to_owned(), Some(vec![]), direction, result);
        let mut builder = FlatBufferBuilder::new();
        let mut external = Sink(Vec::new());
        let bytes = call.encode(&mut builder, &mut external).unwrap();
        assert!(decode(bytes, bytes.len()).is_err());
    }

    let bytes = frame(
        "fetch",
        vec![
            ParameterValue::ByteChunks(vec![Bytes::from_static(b"data")]),
            ParameterValue::ULong(1),
        ],
    );
    assert!(decode(&bytes, bytes.len()).is_err());
}

#[test]
fn aliased_many_parameter_frame_is_rejected_as_an_invalid_executor_operation() {
    let mut bytes = frame("fetch", vec![ParameterValue::String("x".repeat(32 * 1024))]);
    // Mutate an official SDK-encoded test packet, without importing its private
    // generated schema. These offsets construct the fixture, not a decoder.
    let read_u32 =
        |offset| u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    let root = 4 + read_u32(4);
    let displacement = i32::from_le_bytes(bytes[root..root + 4].try_into().unwrap());
    let vtable = usize::try_from(i64::try_from(root).unwrap() - i64::from(displacement)).unwrap();
    let field = |offset| {
        root + usize::from(u16::from_le_bytes(
            bytes[vtable + offset..vtable + offset + 2]
                .try_into()
                .unwrap(),
        ))
    };
    let name_field = field(4);
    let parameters_field = field(6);
    let data = parameters_field + read_u32(parameters_field) + 4;
    let target = data + read_u32(data);
    assert!(name_field < data);
    let added = (8000 - 1) * size_of::<u32>();
    // Expand the official encoder's one-element vector into aliases of its
    // existing verified Parameter. Only forward offsets crossing the insertion move.
    bytes.splice(data + 4..data + 4, vec![0; added]);
    let prefix = u32::from_le_bytes(bytes[..4].try_into().unwrap());
    bytes[..4].copy_from_slice(&(prefix + added as u32).to_le_bytes());
    let name = u32::from_le_bytes(bytes[name_field..name_field + 4].try_into().unwrap());
    bytes[name_field..name_field + 4].copy_from_slice(&(name + added as u32).to_le_bytes());
    bytes[data - 4..data].copy_from_slice(&8000_u32.to_le_bytes());
    for index in 0..8000 {
        let position = data + index * 4;
        let offset = u32::try_from(target + added - position).unwrap();
        bytes[position..position + 4].copy_from_slice(&offset.to_le_bytes());
    }
    assert!(bytes.len() < 69_620);
    let error = decode(&bytes, bytes.len()).err().unwrap();
    assert_eq!(
        error.to_string(),
        "unknown executor call or invalid arguments"
    );
}

#[test]
fn reply_budget_is_independent_of_the_incoming_frame_capacity() {
    let bytes = frame(
        "configure",
        vec![
            ParameterValue::VecBytes(vec![0]),
            ParameterValue::String("http".to_owned()),
            ParameterValue::ULong(64 * 1024),
            ParameterValue::ULong(1),
        ],
    );
    assert!(bytes.len() < 64 * 1024);
    let Call::Configure { reply_limit, .. } = decode(&bytes, bytes.len()).unwrap() else {
        panic!("wrong call")
    };
    assert_eq!(reply_limit, 64 * 1024);
}

#[test]
fn artifact_control_has_bounded_metadata_not_config_payload_or_host_path() {
    let bytes = frame(
        "configure_artifact",
        vec![
            ParameterValue::ULong(8 * 1024 * 1024 + 2048),
            ParameterValue::ULong(16 * 1024 * 1024),
            ParameterValue::VecBytes(vec![42; 32]),
            ParameterValue::String("http".to_owned()),
            ParameterValue::ULong(69_620),
            ParameterValue::ULong(1),
        ],
    );
    assert!(bytes.len() < 1024);
    let Call::ConfigureArtifact {
        length,
        budget,
        digest,
        socket,
        reply_limit,
    } = decode(&bytes, bytes.len()).unwrap()
    else {
        panic!("wrong call")
    };
    assert_eq!(length, 8 * 1024 * 1024 + 2048);
    assert_eq!(budget, 16 * 1024 * 1024);
    assert_eq!(digest, [42; 32]);
    assert_eq!(socket, "http");
    assert_eq!(reply_limit, 69_620);
}

#[test]
fn artifact_control_rejects_invalid_read_budgets_digest_lengths_and_types() {
    for (length, budget, digest_size) in [
        (0, 8, 32),
        (7, 8, 32),
        (16, 8, 32),
        (8, u64::MAX, 32),
        (8, 8, 31),
        (8, 8, 33),
    ] {
        let bytes = frame(
            "configure_artifact",
            vec![
                ParameterValue::ULong(length),
                ParameterValue::ULong(budget),
                ParameterValue::VecBytes(vec![0; digest_size]),
                ParameterValue::String("http".to_owned()),
                ParameterValue::ULong(4096),
                ParameterValue::ULong(1),
            ],
        );
        assert!(decode(&bytes, bytes.len()).is_err());
    }
    let bytes = frame(
        "configure_artifact",
        vec![
            ParameterValue::String("/host/arbitrary/path".to_owned()),
            ParameterValue::ULong(8),
            ParameterValue::VecBytes(vec![0; 32]),
            ParameterValue::String("http".to_owned()),
            ParameterValue::ULong(4096),
            ParameterValue::ULong(1),
        ],
    );
    assert!(decode(&bytes, bytes.len()).is_err());
}
