use super::*;

#[test]
fn one_message_boundary_is_independent_of_connection_close() {
    validate(b"\r\nGET /a HTTP/1.1\r\nHost: example.test\r\n\r\n").unwrap();
    validate(b"GET /a HTTP/1.1\nHost: example.test\n\n").unwrap();
    for connection in ["", "Connection: close\r\n"] {
        let request = format!("GET /a HTTP/1.1\r\nHost: example.test\r\n{connection}\r\n");
        validate(request.as_bytes()).unwrap();
        assert!(
            validate(format!("{request}GET /b HTTP/1.1\r\nHost: example.test\r\n\r\n").as_bytes())
                .is_err()
        );
        assert!(validate(format!("{request}trailing").as_bytes()).is_err());
    }
}

#[test]
fn chunk_extension_and_trailer_newlines_match_hyper_before_dispatch() {
    let head = b"POST /counter HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n";
    for body in [
        &b"0;x=\n\r\n\r\n"[..],
        &b"0\r\n\n"[..],
        &b"0\r\nX-Trailer: yes\n\r\n"[..],
        &b"0\r\nX-Trailer: yes\r\n\n"[..],
    ] {
        let request = [head.as_slice(), body].concat();
        assert!(validate(&request).is_err());
    }
    validate(&[head.as_slice(), b"0;x=yes\r\nX-Trailer: yes\r\n\r\n"].concat()).unwrap();
}

#[test]
fn chunk_extension_budget_is_aggregate_and_matches_hyper_boundary() {
    let head = "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n";
    let allowed = "a".repeat(CHUNK_EXTENSIONS_LIMIT - 1);
    validate(format!("{head}0;{allowed}\r\n\r\n").as_bytes()).unwrap();
    let excessive = "a".repeat(CHUNK_EXTENSIONS_LIMIT);
    assert!(validate(format!("{head}0;{excessive}\r\n\r\n").as_bytes()).is_err());
    let per_chunk = "a".repeat(CHUNK_EXTENSIONS_LIMIT / 2);
    assert!(
        validate(format!("{head}1;{per_chunk}\r\nx\r\n0;{per_chunk}\r\n\r\n").as_bytes()).is_err()
    );
}

#[test]
fn trailer_byte_budget_matches_hyper_boundary() {
    let head = "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nX: ";
    let allowed = "a".repeat(TRAILER_BYTES_LIMIT - 8);
    validate(format!("{head}{allowed}\r\n\r\n").as_bytes()).unwrap();
    let excessive = "a".repeat(TRAILER_BYTES_LIMIT - 7);
    assert!(validate(format!("{head}{excessive}\r\n\r\n").as_bytes()).is_err());
}
#[test]
fn content_length_is_exact_and_binary_body_is_opaque() {
    let mut request = b"POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\n".to_vec();
    request.extend_from_slice(&[0, 255, 0, 128]);
    validate(&request).unwrap();
    assert!(validate(&request[..request.len() - 1]).is_err());
    request.push(0);
    assert!(validate(&request).is_err());
    validate(b"POST / HTTP/1.1\r\nContent-Length: 4, 4\r\nContent-Length: 4\r\n\r\ndata").unwrap();
}

#[test]
fn malformed_or_ambiguous_lengths_are_refused_before_dispatch() {
    for headers in [
        "Content-Length: 4\r\nContent-Length: 5\r\n",
        "Content-Length: +4\r\n",
        "Content-Length: 184467440737095516160\r\n",
        "Transfer-Encoding: chunked\r\nContent-Length: 0\r\n",
        "Transfer-Encoding: chunked, gzip\r\n",
        "Transfer-Encoding: gzip\r\n",
    ] {
        assert!(validate(format!("POST / HTTP/1.1\r\n{headers}\r\n0\r\n\r\n").as_bytes()).is_err());
    }
    assert!(validate(b"GARBAGE\r\n\r\n").is_err());
    assert!(validate(b"GET / HTTP/1.1\r\nHost: incomplete").is_err());
}

#[test]
fn chunked_message_extensions_and_trailers_have_an_exact_end() {
    let request = b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4;note=yes\r\ndata\r\n0\r\nX-Trailer: yes\r\n\r\n";
    validate(request).unwrap();
    for end in 0..request.len() {
        assert!(validate(&request[..end]).is_err(), "accepted prefix {end}");
    }
    let mut trailing = request.to_vec();
    trailing.extend_from_slice(b"GET /hidden HTTP/1.1\r\n\r\n");
    assert!(validate(&trailing).is_err());
    assert!(
        validate(b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nffffffffffffffff\r\n")
            .is_err()
    );
    assert!(
        validate(b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ndataXX0\r\n\r\n")
            .is_err()
    );
    assert!(validate(b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n\r\n\r\n").is_err());
    validate(b"POST / HTTP/1.1\r\nTransfer-Encoding: gzip, chunked\r\n\r\n0\r\n\r\n").unwrap();
}
