# raft-example

```shell
./raft-example --node-id node1 --raft-port 2222 --http-port 8222
```

```shell
./raft-example --node-id node2 --raft-port 2223 --http-port 8223
```

```shell
./raft-example --node-id node3 --raft-port 2224 --http-port 8224
```

```shell
curl 'localhost:8222/join?followerAddr=localhost:2223&followerId=node2'
```

```shell
curl 'localhost:8222/join?followerAddr=localhost:2224&followerId=node3'
```

```shell
curl -X POST 'localhost:8223/set' -d '{"key": "x", "value": "hello"}' -H 'content-type: application/json'
```

```shell
curl -X POST 'localhost:8222/set' -d '{"key": "x", "value": "hi"}' -H 'content-type: application/json'
```

```shell
curl 'localhost:8222/get?key=x'
```

```shell
curl 'localhost:8223/get?key=x'
```

```shell
curl 'localhost:8224/get?key=xx'
```
