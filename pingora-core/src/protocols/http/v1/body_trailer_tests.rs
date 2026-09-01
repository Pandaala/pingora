// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use tokio_test::io::Builder;

#[tokio::test]
async fn chunked_trailers_are_parsed_with_duplicates_and_overread_preserved() {
    let mut io = Builder::new()
        .read(b"3\r\nabc\r\n0\r\nx-a: 1\r\n")
        .read(b"x-a: 2\r\nx-b: final\r\n\r\nNEXT")
        .build();
    let mut reader = BodyReader::new(true);
    reader.init_chunked(b"");
    while !reader.body_done() {
        let _ = reader.read_body(&mut io).await.unwrap();
    }

    let trailers = reader
        .take_trailers()
        .expect("real trailers must be retained");
    assert_eq!(
        trailers
            .get_all("x-a")
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>(),
        ["1", "2"]
    );
    assert_eq!(trailers["x-b"], "final");
    assert_eq!(reader.get_body_overread(), Some(&b"NEXT"[..]));
}

#[test]
fn trailer_parser_rejects_malformed_forbidden_count_and_size_boundaries() {
    for invalid in [
        &b"missing-colon\r\n\r\n"[..],
        &b"content-length: 1\r\n\r\n"[..],
        &b"connection: close\r\n\r\n"[..],
        &b"host: example.com\r\n\r\n"[..],
    ] {
        assert!(BodyReader::parse_trailers(invalid).is_err());
    }

    let mut fields = Vec::new();
    for _ in 0..MAX_HEADERS {
        fields.extend_from_slice(b"x-duplicate: ok\r\n");
    }
    fields.extend_from_slice(b"\r\n");
    assert_eq!(
        BodyReader::parse_trailers(&fields).unwrap().len(),
        MAX_HEADERS
    );
    fields.splice(
        fields.len() - 2..fields.len() - 2,
        b"x-over: no\r\n".iter().copied(),
    );
    assert!(BodyReader::parse_trailers(&fields).is_err());

    let oversized = vec![b'a'; TRAILER_SIZE_LIMIT + 1];
    assert!(BodyReader::parse_trailers(&oversized).is_err());
}

#[tokio::test]
async fn chunked_trailer_writer_preserves_duplicates_and_rejects_non_chunked() {
    let expected = b"0\r\nx-a: 1\r\nx-a: 2\r\n\r\n";
    let mut io = Builder::new().write(expected).build();
    let mut writer = BodyWriter::new();
    writer.init_chunked();
    let mut trailers = HeaderMap::new();
    trailers.append("x-a", HeaderValue::from_static("1"));
    trailers.append("x-a", HeaderValue::from_static("2"));
    assert_eq!(
        writer.write_trailers(&mut io, &trailers).await.unwrap(),
        Some(0)
    );
    assert_eq!(writer.body_mode, BodyMode::Complete(0));

    let mut no_write = Builder::new().build();
    let mut content_length = BodyWriter::new();
    content_length.init_content_length(0);
    assert!(content_length
        .write_trailers(&mut no_write, &trailers)
        .await
        .is_err());

    let mut too_many = HeaderMap::new();
    for _ in 0..=MAX_HEADERS {
        too_many.append("x-duplicate", HeaderValue::from_static("ok"));
    }
    let mut no_write = Builder::new().build();
    let mut writer = BodyWriter::new();
    writer.init_chunked();
    assert!(writer
        .write_trailers(&mut no_write, &too_many)
        .await
        .is_err());

    let mut too_large = HeaderMap::new();
    too_large.insert(
        "x-large",
        HeaderValue::from_bytes(&vec![b'a'; TRAILER_SIZE_LIMIT]).unwrap(),
    );
    let mut no_write = Builder::new().build();
    let mut writer = BodyWriter::new();
    writer.init_chunked();
    assert!(writer
        .write_trailers(&mut no_write, &too_large)
        .await
        .is_err());
}

#[tokio::test]
async fn trailerless_chunked_finish_writes_the_standard_empty_terminator() {
    let mut io = Builder::new().write(b"0\r\n\r\n").build();
    let mut writer = BodyWriter::new();
    writer.init_chunked();
    assert_eq!(writer.finish(&mut io).await.unwrap(), Some(0));
    assert_eq!(writer.body_mode, BodyMode::Complete(0));
}
