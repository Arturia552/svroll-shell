use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use super::tcp_client_data::RequestCodec;

async fn start_tcp_client(host: String) {
    if let Ok(conn) = TcpStream::connect(host).await {
        tokio::spawn(async move {
            process_client(conn).await;
        });
    } else {
        println!("Tcp server 连接报错");
    }
}

async fn process_client(tcp_stream: TcpStream) {
    let (client_reader, _) = tcp_stream.into_split();

    let mut frame_reader = FramedRead::new(client_reader, RequestCodec);

    loop {
        match frame_reader.next().await {
            None => {
                break;
            }
            Some(Err(_e)) => {
                break;
            }
            Some(Ok(req_resp)) => {
                println!("Received request: {:?}", req_resp);
            }
        }
    }
}
