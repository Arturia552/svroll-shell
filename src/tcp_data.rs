use bytes::Buf;
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

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
        if (src.len() < 4) {
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