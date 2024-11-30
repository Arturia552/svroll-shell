use bytes::Buf;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, FramedRead};

pub struct TcpConn {}

#[derive(Debug)]
pub struct RequestData {
    data: String,
}
#[derive(Debug)]
pub struct ResponseData {
    data: String,
}

pub struct RequestCodec;

impl Decoder for RequestCodec {
    type Item = RequestData;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let len = src.get_u16() as usize;

        if src.len() < len {
            src.reserve(2 + len);
            return Ok(None);
        }

        let data = src.split_to(len);
        let req: RequestData = RequestData {
            data: "".to_string(),
        };
        Ok(Some(req))
    }
}

pub struct ResponseCodec;

impl Encoder<ResponseData> for ResponseCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: ResponseData, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        todo!()
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
