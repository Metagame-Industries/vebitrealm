use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClientBuilder;
use jsonrpsee::rpc_params;
use parity_scale_codec::{Decode, Encode};
use tokio_postgres::{Client, NoTls};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use vemodel::{
    Method, VeArticle, VeComment, VeSubspace, PREFIX_ARTICLE_KEY, PREFIX_COMMENT_KEY,
    PREFIX_SUBSPACE_KEY,
};

async fn setup_database(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    // Create tables with proper schema matching the Rust structs
    client.batch_execute("
        CREATE TABLE IF NOT EXISTS subspaces (
            id BIGINT PRIMARY KEY,
            title VARCHAR NOT NULL,
            slug VARCHAR NOT NULL,
            description TEXT,
            banner VARCHAR,
            status SMALLINT NOT NULL,
            weight SMALLINT NOT NULL,
            created_time BIGINT NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS articles (
            id BIGINT PRIMARY KEY,
            title VARCHAR NOT NULL,
            content TEXT NOT NULL,
            author_id BIGINT NOT NULL,
            author_nickname VARCHAR NOT NULL,
            subspace_id BIGINT NOT NULL,
            ext_link VARCHAR,
            status SMALLINT NOT NULL,
            weight SMALLINT NOT NULL,
            created_time BIGINT NOT NULL,
            updated_time BIGINT NOT NULL,
            FOREIGN KEY (subspace_id) REFERENCES subspaces(id)
        );
        
        CREATE TABLE IF NOT EXISTS comments (
            id BIGINT PRIMARY KEY,
            content TEXT NOT NULL,
            author_id BIGINT NOT NULL,
            author_nickname VARCHAR NOT NULL,
            post_id BIGINT NOT NULL,
            status SMALLINT NOT NULL,
            weight SMALLINT NOT NULL,
            created_time BIGINT NOT NULL,
            FOREIGN KEY (post_id) REFERENCES articles(id)
        );
    ").await?;
    
    Ok(())
}

async fn handle_database_operation(
    client: &Client,
    model: &str,
    method: Method,
    value: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    match (model, method) {
        ("subspace", Method::Create | Method::Update) => {
            let subspace: VeSubspace = serde_json::from_value(value.clone())?;
            client.execute(
                "INSERT INTO subspaces (id, title, slug, description, banner, status, weight, created_time)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT (id) DO UPDATE SET
                    title = $2,
                    slug = $3,
                    description = $4,
                    banner = $5,
                    status = $6,
                    weight = $7,
                    created_time = $8",
                &[
                    &(subspace.id as i64),
                    &subspace.title,
                    &subspace.slug,
                    &subspace.description,
                    &subspace.banner,
                    &(subspace.status as i16),
                    &(subspace.weight as i16),
                    &(subspace.created_time as i64),
                ],
            ).await?;
            println!("Upserted subspace: {}", subspace.id);
        },
        ("article", Method::Create | Method::Update) => {
            let article: VeArticle = serde_json::from_value(value.clone())?;
            client.execute(
                "INSERT INTO articles (id, title, content, author_id, author_nickname, subspace_id, 
                                     ext_link, status, weight, created_time, updated_time)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                 ON CONFLICT (id) DO UPDATE SET
                    title = $2,
                    content = $3,
                    author_id = $4,
                    author_nickname = $5,
                    subspace_id = $6,
                    ext_link = $7,
                    status = $8,
                    weight = $9,
                    created_time = $10,
                    updated_time = $11",
                &[
                    &(article.id as i64),
                    &article.title,
                    &article.content,
                    &(article.author_id as i64),
                    &article.author_nickname,
                    &(article.subspace_id as i64),
                    &article.ext_link,
                    &(article.status as i16),
                    &(article.weight as i16),
                    &(article.created_time as i64),
                    &(article.updated_time as i64),
                ],
            ).await?;
            println!("Upserted article: {}", article.id);
        },
        ("comment", Method::Create | Method::Update) => {
            let comment: VeComment = serde_json::from_value(value.clone())?;
            client.execute(
                "INSERT INTO comments (id, content, author_id, author_nickname, post_id, 
                                     status, weight, created_time)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT (id) DO UPDATE SET
                    content = $2,
                    author_id = $3,
                    author_nickname = $4,
                    post_id = $5,
                    status = $6,
                    weight = $7,
                    created_time = $8",
                &[
                    &(comment.id as i64),
                    &comment.content,
                    &(comment.author_id as i64),
                    &comment.author_nickname,
                    &(comment.post_id as i64),
                    &(comment.status as i16),
                    &(comment.weight as i16),
                    &(comment.created_time as i64),
                ],
            ).await?;
            println!("Upserted comment: {}", comment.id);
        },
        (model, Method::Delete) => {
            let id = value.as_i64().unwrap();
            let table_name = match model {
                "subspace" => "subspaces",
                "article" => "articles",
                "comment" => "comments",
                _ => return Err("Invalid model type".into()),
            };
            let query = format!("DELETE FROM {} WHERE id = $1", table_name);
            client.execute(&query, &[&id]).await?;
            println!("Deleted {} record: {}", table_name, id);
        },
        _ => return Err("Invalid operation".into()),
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::channel(100);

    // PostgreSQL connection
    let postgres_config = "host=localhost port=5432 user=postgres password=your_password dbname=ve_db";
    let (client, connection) = tokio_postgres::connect(postgres_config, NoTls).await?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {}", e);
        }
    });

    // Set up database tables
    setup_database(&client).await?;

    // Spawn a task for PostgreSQL operations
    let db_client = client.clone();
    tokio::spawn(async move {
        while let Some((model, method, value)) = rx.recv().await {
            if let Err(e) = handle_database_operation(&db_client, &model, method, &value).await {
                eprintln!("Database operation error: {}", e);
            }
        }
    });

    // Main task for RPC querying
    let http_client = HttpClientBuilder::default().build("http://localhost:9944")?;

    let avs_id = "5FsXfPrUDqq6abYccExCTUxyzjYaaYTr5utLx2wwdBv1m8R8";
    let mut sentinel: u64 = 0;
    loop {
        println!("==> sentinel: {}", sentinel);
        let params = rpc_params![
            avs_id,
            "get_from_common_key",
            hex::encode(sentinel.encode())
        ];

        let res: serde_json::Value = http_client.request("nucleus_post", params).await?;
        let res = res.as_str().expect("a str res");
        let bytes = hex::decode(res).expect("Invalid hex string");
        let res = <Result<Vec<(u64, Method, Vec<u8>)>, String>>::decode(&mut &bytes[..]).unwrap();

        for (reqnum, method, key) in res? {
            match slice_to_array(&key[..5]).unwrap() {
                PREFIX_SUBSPACE_KEY => {
                    let id = vec_to_u64(&key[5..]);
                    match method {
                        Method::Create | Method::Update => {
                            let params = rpc_params![avs_id, "get_subspace", hex::encode(id.encode())];
                            let res: serde_json::Value = http_client.request("nucleus_get", params).await?;
                            let res = res.as_str().expect("a str res");
                            let bytes = hex::decode(res).expect("Invalid hex string");
                            let result = <Result<Option<VeSubspace>, String>>::decode(&mut &bytes[..]).unwrap();
                            if let Ok(Some(sb)) = result {
                                let json_value = serde_json::to_value(&sb)?;
                                tx.send(("subspace", method, json_value)).await?;
                            }
                        }
                        Method::Delete => {
                            let json_value = serde_json::to_value(&id)?;
                            tx.send(("subspace", method, json_value)).await?;
                        }
                    }
                }
                PREFIX_ARTICLE_KEY => {
                    let id = vec_to_u64(&key[5..]);
                    match method {
                        Method::Create | Method::Update => {
                            let params = rpc_params![avs_id, "get_article", hex::encode(id.encode())];
                            let res: serde_json::Value = http_client.request("nucleus_get", params).await?;
                            let res = res.as_str().expect("a str res");
                            let bytes = hex::decode(res).expect("Invalid hex string");
                            let result = <Result<Option<VeArticle>, String>>::decode(&mut &bytes[..]).unwrap();
                            if let Ok(Some(article)) = result {
                                let json_value = serde_json::to_value(&article)?;
                                tx.send(("article", method, json_value)).await?;
                            }
                        }
                        Method::Delete => {
                            let json_value = serde_json::to_value(&id)?;
                            tx.send(("article", method, json_value)).await?;
                        }
                    }
                }
                PREFIX_COMMENT_KEY => {
                    let id = vec_to_u64(&key[5..]);
                    match method {
                        Method::Create | Method::Update => {
                            let params = rpc_params![avs_id, "get_comment", hex::encode(id.encode())];
                            let res: serde_json::Value = http_client.request("nucleus_get", params).await?;
                            let res = res.as_str().expect("a str res");
                            let bytes = hex::decode(res).expect("Invalid hex string");
                            let result = <Result<Option<VeComment>, String>>::decode(&mut &bytes[..]).unwrap();
                            if let Ok(Some(comment)) = result {
                                let json_value = serde_json::to_value(&comment)?;
                                tx.send(("comment", method, json_value)).await?;
                            }
                        }
                        Method::Delete => {
                            let json_value = serde_json::to_value(&id)?;
                            tx.send(("comment", method, json_value)).await?;
                        }
                    }
                }
                _ => {}
            }
            sentinel = reqnum;
        }

        sleep(Duration::from_secs(5)).await;
    }
}

fn vec_to_u64(v: &[u8]) -> u64 {
    let mut array = [0u8; 8];
    let len = std::cmp::min(v.len(), 8);
    array[..len].copy_from_slice(&v[..len]);
    u64::from_be_bytes(array)
}

fn slice_to_array(slice: &[u8]) -> Result<&[u8; 5], &str> {
    slice.try_into().map_err(|_| "Slice must be 5 bytes long")
}

