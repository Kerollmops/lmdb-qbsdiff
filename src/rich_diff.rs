use std::borrow::Cow;
use heed::{BytesEncode, BytesDecode};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RichDiff<'a> {
    Addition(&'a [u8]),
    Patch(&'a [u8]),
    Deletion,
}

pub struct RichCodec;

impl<'a> BytesEncode<'a> for RichCodec {
    type EItem = RichDiff<'a>;

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        match item {
            RichDiff::Addition(bytes) => {
                let mut vec = Vec::with_capacity(1 + bytes.len());
                vec.push(b'+');
                vec.extend_from_slice(bytes);
                Some(Cow::Owned(vec))
            },
            RichDiff::Patch(bytes) => {
                let mut vec = Vec::with_capacity(1 + bytes.len());
                vec.push(b'~');
                vec.extend_from_slice(bytes);
                Some(Cow::Owned(vec))
            },
            RichDiff::Deletion => Some(Cow::Borrowed(&[b'-'])),
        }
    }
}

impl<'a> BytesDecode<'a> for RichCodec {
    type DItem = RichDiff<'a>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        match bytes.split_first() {
            Some((b'+', bytes)) => Some(RichDiff::Addition(bytes)),
            Some((b'~', bytes)) => Some(RichDiff::Patch(bytes)),
            Some((b'-', _)) => Some(RichDiff::Deletion),
            _ => None,
        }
    }
}
