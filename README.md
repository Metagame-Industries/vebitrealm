
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



## APPENDIX: Upload Video to IPFS

```
Uploading a video file to IPFS involves adding the file to the IPFS network using an IPFS node or an API like Infura or Web3.Storage. Here's an example using the `ipfs-http-client` JavaScript library:

### Prerequisites
1. Install the `ipfs-http-client` library:
   ```bash
   npm install ipfs-http-client
   ```

2. Set up an IPFS node or use a public gateway/API like [Infura](https://infura.io) or [Web3.Storage](https://web3.storage).

### Example Code
Here's an example script to upload a video file:

```javascript
// Import the IPFS HTTP client
import { create } from 'ipfs-http-client';
import fs from 'fs';

// Connect to an IPFS node (adjust the URL for your node or API endpoint)
const ipfs = create({ url: 'https://ipfs.infura.io:5001/api/v0' });

async function uploadVideo(filePath) {
  try {
    // Read the video file
    const file = fs.readFileSync(filePath);

    // Add the file to IPFS
    const { cid } = await ipfs.add(file);

    console.log('Video uploaded to IPFS!');
    console.log('CID:', cid.toString());
    console.log(`Gateway URL: https://ipfs.io/ipfs/${cid.toString()}`);
  } catch (error) {
    console.error('Error uploading video:', error);
  }
}

// Replace with the path to your video file
const videoPath = './path/to/your-video.mp4';

uploadVideo(videoPath);
```

### Explanation
1. **Connection to IPFS**:
   - The `create` function establishes a connection to an IPFS node or gateway.
   - The URL for the node or gateway must be configured based on your setup.

2. **File Upload**:
   - The `fs.readFileSync` reads the video file into a buffer.
   - The `ipfs.add` method uploads the file to IPFS, returning a `cid` (Content Identifier).

3. **Access the File**:
   - The file can be accessed through a public IPFS gateway using the returned CID (e.g., `https://ipfs.io/ipfs/<CID>`).

### Notes
- If using a local IPFS node, make sure it’s running.
- Consider using APIs like Web3.Storage for additional features like pinning and simplified upload management.
- If the video is large, you might need to use streaming or chunking options provided by the `ipfs-http-client`.

```
