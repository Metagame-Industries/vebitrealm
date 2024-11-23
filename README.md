
## Model

see vemodel crate.

## API

jsonrpc method call

```
add_user(sb: BitUser)
update_user(sb: BitUser)
delete_user(id: u64)

add_video(sb: BitVideo)
update_video(sb: BitVideo)
delete_video(id: u64)

add_like(sb: BitLike)
update_like(sb: BitLike)
delete_like(id: u64)

add_comment(co: BitComment)
update_comment(co: BitComment)
delete_comment(id: u64)
```


## Test Method

Test like follows:

```
----
User 

curl localhost:9944 -H 'Content-Type: application/json' -XPOST -d '{"jsonrpc":"2.0", "id":"whatever", "method":"nucleus_post", "params": ["5FsXfPrUDqq6abYccExCTUxyzjYaaYTr5utLx2wwdBv1m8R8", "add_user", "01000000000000004c78646e6468616865756e636864616865696e642074656c656772616d1c6d696b65313233d202964900000000"]}'

curl localhost:9944 -H 'Content-Type: application/json' -XPOST -d '{"jsonrpc":"2.0", "id":"whatever", "method":"nucleus_get", "params": ["5FsXfPrUDqq6abYccExCTUxyzjYaaYTr5utLx2wwdBv1m8R8", "get_user", "0100000000000000"]}'

----
Video

curl localhost:9944 -H 'Content-Type: application/json' -XPOST -d '{"jsonrpc":"2.0", "id":"whatever", "method":"nucleus_post", "params": ["5FsXfPrUDqq6abYccExCTUxyzjYaaYTr5utLx2wwdBv1m8R8", "add_video", "010000000000000028766964656f207465737454746869732069732061207465737420766964656f2e402f706174682f766964656f312e6d703458706174682f766964656f315f62616e6e65722e6a7067d202964900000000"]}'

----
Like

curl localhost:9944 -H 'Content-Type: application/json' -XPOST -d '{"jsonrpc":"2.0", "id":"whatever", "method":"nucleus_post", "params": ["5FsXfPrUDqq6abYccExCTUxyzjYaaYTr5utLx2wwdBv1m8R8", "add_like", "0100000000000000640000000000000003000000000000000100000000000000d202964900000000"]}'

----
Comment

curl localhost:9944 -H 'Content-Type: application/json' -XPOST -d '{"jsonrpc":"2.0", "id":"whatever", "method":"nucleus_post", "params": ["5FsXfPrUDqq6abYccExCTUxyzjYaaYTr5utLx2wwdBv1m8R8", "add_comment", "0100000000000000640000000000000003000000000000004469206c696b65207468697320766964656fd202964900000000"]}'


```
