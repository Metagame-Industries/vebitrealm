use parity_scale_codec::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Decode, Encode, Deserialize, Serialize)]
pub enum Method {
    Create,
    Update,
    Delete,
}

#[derive(Debug, Decode, Encode, Deserialize, Serialize)]
pub struct BitUser {
    pub id: u64,
    pub handle: String, // the tg handle
    pub source: String, // default tg
    pub nickname: String,
    pub created_time: i64,
}

#[derive(Debug, Decode, Encode, Deserialize, Serialize)]
pub struct BitVideo {
    pub id: u64,
    pub title: String,
    pub description: String,
    pub url: String,
    pub banner: String, // banner pic url
    pub created_time: i64,
}

#[derive(Debug, Decode, Encode, Deserialize, Serialize)]
pub struct BitLike {
    pub id: u64,
    pub video_id: u64,
    pub user_id: u64,
    pub likenum: u64,
    pub created_time: i64,
}

#[derive(Debug, Decode, Encode, Deserialize, Serialize)]
pub struct BitComment {
    pub id: u64,
    pub video_id: u64,
    pub user_id: u64,
    pub content: String,
    pub created_time: i64,
}

pub const PREFIX_USER_KEY: &[u8; 5] = b"busr:";
pub const PREFIX_VIDEO_KEY: &[u8; 5] = b"bvid:";
pub const PREFIX_LIKE_KEY: &[u8; 5] = b"blik:";
pub const PREFIX_COMMENT_KEY: &[u8; 5] = b"bcom:";

pub const REQNUM_KEY: &[u8; 7] = b"_reqnum";
pub const COMMON_KEY: &[u8; 7] = b"_common";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
