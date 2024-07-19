use tokio::net::TcpStream;
use tokio_stream::{self as stream, StreamExt};
use tokio_util::codec::{Decoder, FramedRead};

use crate::tcp_data::RequestCodec;

async fn start_tcp_client(host: String) {
    let conn = TcpStream::connect(host).await.unwrap();

    tokio::spawn(async move {
        process_client(conn).await;
    });
}

async fn process_client(tcp_stream: TcpStream) {
    let (client_reader, _) = tcp_stream.into_split();

    let mut frame_reader = FramedRead::new(client_reader, RequestCodec);
    let mut buf: bytes::BytesMut = bytes::BytesMut::new();

    loop {
        match frame_reader.next().await {
            None => {
                break;
            }
            Some(Err(e)) => {
                break;
            }
            Some(Ok(req_resp)) => {
                println!("Received request: {:?}", req_resp);
            }
        }
    }
}
