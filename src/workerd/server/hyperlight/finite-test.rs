use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use super::*;

#[test]
fn exchange_has_finite_eof_and_preserves_binary_bytes() {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let (mut exchange, output) = Exchange::new(vec![0, 255, 0, 128], 4);
            let mut input = Vec::new();
            exchange.read_to_end(&mut input).await.unwrap();
            assert_eq!(input, [0, 255, 0, 128]);
            exchange.write_all(&[1, 2, 3, 4]).await.unwrap();
            assert_eq!(*output.lock().unwrap(), [1, 2, 3, 4]);
            assert_eq!(
                exchange.write_all(&[5]).await.unwrap_err().kind(),
                io::ErrorKind::InvalidData,
            );
            assert_eq!(*output.lock().unwrap(), [1, 2, 3, 4]);
        });
}
