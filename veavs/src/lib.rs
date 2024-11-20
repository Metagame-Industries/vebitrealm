use parity_scale_codec::{Decode, Encode};
use std::isize;
use vrs_core_sdk::{get, post, storage};

use vemodel::{
    Method, BitUser, BitVideo, BitLike, BitComment, COMMON_KEY, PREFIX_USER_KEY, PREFIX_COMMENT_KEY,
    PREFIX_VIDEO_KEY, PREFIX_LIKE_KEY, REQNUM_KEY,
};

// subspace
#[post]
pub fn add_user(sb: BitUser) -> Result<(), String> {
    let max_id = get_max_id(PREFIX_USER_KEY);
    // update the id field from the avs
    sb.id = max_id;
    let key = build_key(PREFIX_USER_KEY, max_id);
    storage::put(&key, sb.encode()).map_err(|e| e.to_string())?;

    add_to_common_key(Method::Create, key)?;

    Ok(())
}

#[post]
pub fn update_user(sb: BitUser) -> Result<(), String> {
    let id = sb.id;
    let key = build_key(PREFIX_USER_KEY, id);
    storage::put(&key, sb.encode()).map_err(|e| e.to_string())?;

    add_to_common_key(Method::Update, key)?;

    Ok(())
}

#[post]
pub fn delete_user(id: u64) -> Result<(), String> {
    let key = build_key(PREFIX_USER_KEY, id);
    storage::del(&key).map_err(|e| e.to_string())?;

    add_to_common_key(Method::Delete, key)?;

    Ok(())
}

#[get]
pub fn get_user(id: u64) -> Result<Option<BitUser>, String> {
    let key = build_key(PREFIX_USER_KEY, id);
    let r = storage::get(&key).map_err(|e| e.to_string())?;
    let instance = r.map(|d| BitUser::decode(&mut &d[..]).unwrap());
    Ok(instance)
}

// video
#[post]
pub fn add_video(mut sb: BitVideo) -> Result<(), String> {
    let max_id = get_max_id(PREFIX_VIDEO_KEY);
    // update the id field from the avs
    sb.id = max_id;
    let key = build_key(PREFIX_VIDEO_KEY, max_id);
    storage::put(&key, sb.encode()).map_err(|e| e.to_string())?;
    add_to_common_key(Method::Create, key)?;

    Ok(())
}

#[post]
pub fn update_video(sb: BitVideo) -> Result<(), String> {
    let id = sb.id;
    let key = build_key(PREFIX_VIDEO_KEY, id);
    storage::put(&key, sb.encode()).map_err(|e| e.to_string())?;
    add_to_common_key(Method::Update, key)?;

    Ok(())
}

#[post]
pub fn delete_video(id: u64) -> Result<(), String> {
    let key = build_key(PREFIX_VIDEO_KEY, id);
    storage::del(&key).map_err(|e| e.to_string())?;
    add_to_common_key(Method::Delete, key)?;

    Ok(())
}

#[get]
pub fn get_video(id: u64) -> Result<Option<BitVideo>, String> {
    let key = build_key(PREFIX_VIDEO_KEY, id);
    let r = storage::get(&key).map_err(|e| e.to_string())?;
    let instance = r.map(|d| BitVideo::decode(&mut &d[..]).unwrap());
    Ok(instance)
}

// like
#[post]
pub fn add_like(mut sb: BitLike) -> Result<(), String> {
    let max_id = get_max_id(PREFIX_LIKE_KEY);
    // update the id field from the avs
    sb.id = max_id;
    let key = build_key(PREFIX_LIKE_KEY, max_id);
    storage::put(&key, sb.encode()).map_err(|e| e.to_string())?;
    add_to_common_key(Method::Create, key)?;

    Ok(())
}

#[post]
pub fn update_like(sb: BitLike) -> Result<(), String> {
    let id = sb.id;
    let key = build_key(PREFIX_LIKE_KEY, id);
    storage::put(&key, sb.encode()).map_err(|e| e.to_string())?;
    add_to_common_key(Method::Update, key)?;

    Ok(())
}

#[post]
pub fn delete_like(id: u64) -> Result<(), String> {
    let key = build_key(PREFIX_LIKE_KEY, id);
    storage::del(&key).map_err(|e| e.to_string())?;
    add_to_common_key(Method::Delete, key)?;

    Ok(())
}

#[get]
pub fn get_like(id: u64) -> Result<Option<BitLike>, String> {
    let key = build_key(PREFIX_LIKE_KEY, id);
    let r = storage::get(&key).map_err(|e| e.to_string())?;
    let instance = r.map(|d| BitLike::decode(&mut &d[..]).unwrap());
    Ok(instance)
}


// comment
#[post]
pub fn add_comment(mut sb: BitComment) -> Result<(), String> {
    let max_id = get_max_id(PREFIX_COMMENT_KEY);
    // update the id field from the avs
    sb.id = max_id;
    let key = build_key(PREFIX_COMMENT_KEY, max_id);
    storage::put(&key, sb.encode()).map_err(|e| e.to_string())?;
    add_to_common_key(Method::Create, key)?;

    Ok(())
}

#[post]
pub fn update_comment(sb: BitComment) -> Result<(), String> {
    let id = sb.id;
    let key = build_key(PREFIX_COMMENT_KEY, id);
    storage::put(&key, sb.encode()).map_err(|e| e.to_string())?;
    add_to_common_key(Method::Update, key)?;

    Ok(())
}

#[post]
pub fn delete_comment(id: u64) -> Result<(), String> {
    let key = build_key(PREFIX_COMMENT_KEY, id);
    storage::del(&key).map_err(|e| e.to_string())?;
    add_to_common_key(Method::Delete, key)?;

    Ok(())
}

#[get]
pub fn get_comment(id: u64) -> Result<Option<BitComment>, String> {
    let key = build_key(PREFIX_COMMENT_KEY, id);
    let r = storage::get(&key).map_err(|e| e.to_string())?;
    let instance = r.map(|d| BitComment::decode(&mut &d[..]).unwrap());
    Ok(instance)
}

#[get]
pub fn check_all_range() -> Result<(), String> {
    check_range(PREFIX_SUBSPACE_KEY);
    check_range(PREFIX_ARTICLE_KEY);
    check_range(PREFIX_COMMENT_KEY);
    Ok(())
}

//
//
fn add_to_common_key(method: Method, model_ins: Vec<u8>) -> Result<(), String> {
    let reqnum = get_reqnum();

    let res = storage::get(COMMON_KEY).map_err(|e| e.to_string())?;
    if let Some(res) = res {
        let mut avec = Vec::<(u64, Method, Vec<u8>)>::decode(&mut &res[..]).unwrap();
        // insert new tuple item
        avec.push((reqnum, method, model_ins));
        // write back
        _ = storage::put(COMMON_KEY, avec.encode()).map_err(|e| e.to_string());
    } else {
        let avec = vec![(reqnum, method, model_ins)];
        _ = storage::put(COMMON_KEY, avec.encode()).map_err(|e| e.to_string());
    }

    Ok(())
}

#[post]
pub fn get_from_common_key(sentinel: u64) -> Result<Vec<(u64, Method, Vec<u8>)>, String> {
    let res = storage::get(COMMON_KEY).map_err(|e| e.to_string())?;
    if let Some(res) = res {
        let mut avec = Vec::<(u64, Method, Vec<u8>)>::decode(&mut &res[..]).unwrap();
        let mut index: isize = -1;
        for (i, &(reqnum, _, _)) in avec.iter().enumerate() {
            if reqnum <= sentinel {
                index = i as isize;
            } else {
                break;
            }
        }
        let last_part = avec.split_off((index + 1) as usize);

        _ = storage::put(COMMON_KEY, last_part.encode()).map_err(|e| e.to_string());
        return Ok(last_part);
    }

    Ok(vec![])
}

fn get_max_id(prefix: &[u8; 5]) -> u64 {
    let max_id_key = [prefix, &u64::MAX.to_be_bytes()[..]].concat();
    let max_id = match storage::search(&max_id_key, storage::Direction::Reverse)
        .map_err(|e| e.to_string())
    {
        Ok(Some((id, _))) => {
            println!("==-->> max_id_key: {:?}", id);
            if let Ok(id) = id[5..].try_into() {
                u64::from_be_bytes(id) + 1
            } else {
                1u64
            }
        }
        Ok(None) => 1u64,
        Err(_) => 1u64,
    };
    println!("==-->> the next max id is: {}", max_id);

    max_id
}

fn check_range(prefix: &[u8; 5]) {
    match storage::get_range(&prefix, storage::Direction::Forward, 100).map_err(|e| e.to_string()) {
        Ok(vec) => {
            println!("{:?}", vec)
        }
        Err(e) => {
            println!("{:?}", e)
        }
    };
}

fn build_key(prefix: &[u8; 5], id: u64) -> Vec<u8> {
    [prefix, &id.to_be_bytes()[..]].concat()
}

fn get_reqnum() -> u64 {
    let res = storage::get(REQNUM_KEY)
        .map_err(|e| e.to_string())
        .expect("error in storage get");
    let reqnum = if let Some(res) = res {
        // XXX: notice that the SCALE use the little endian format
        let reqnum: u64 = u64::from_le_bytes(TryInto::<[u8; 8]>::try_into(res).unwrap());
        println!("==> current reqnum: {:?}", reqnum);

        // increase reqnum on every request of reqnum
        let reqnum = reqnum + 1;
        _ = storage::put(REQNUM_KEY, reqnum.encode()).map_err(|e| e.to_string());

        reqnum
    } else {
        // initialize it on start
        let reqnum = 1;
        _ = storage::put(REQNUM_KEY, reqnum.encode()).map_err(|e| e.to_string());

        reqnum
    };

    reqnum
}
