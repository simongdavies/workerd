use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;

use super::*;

#[test]
fn complete_preserves_existing_status_and_exact_json_device_payload_without_lf() {
    let (write, mut read) = UnixStream::pair().unwrap();
    let descriptor: OwnedFd = write.into();
    let mut queue = Queue {
        file: descriptor.into(),
        buffer: Vec::new(),
    };
    let json =
        br#"{"protocol_version":1,"request_id":"event-1","outcome":"exception","retry":false}"#;
    queue.complete(0, json).unwrap();
    let mut result = vec![0; size_of::<i32>() + json.len()];
    read.read_exact(&mut result).unwrap();
    assert_eq!(&result[..size_of::<i32>()], &0_i32.to_ne_bytes());
    assert_eq!(&result[size_of::<i32>()..], json);
    assert!(!result[size_of::<i32>()..].contains(&b'\n'));
    queue.complete(-1, &[]).unwrap();
    let mut failure = [0; size_of::<i32>()];
    read.read_exact(&mut failure).unwrap();
    assert_eq!(failure, (-1_i32).to_ne_bytes());
}

#[test]
fn complete_failure_is_reported_without_response_retry_or_fallback() {
    let mut queue = Queue {
        file: File::options().write(true).open("/dev/full").unwrap(),
        buffer: Vec::new(),
    };
    let error = queue
        .complete(0, br#"{"request_id":"event-1"}"#)
        .unwrap_err();
    assert!(error.to_string().contains("writing Hyperlight call result"));
}
