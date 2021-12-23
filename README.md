# redis-proxy

A Redis proxy built on TiKV

## How to use

Build the proxy:

```sh
go build -o main main.go
```

Run the proxy to connect the cluster whose PD adress is `127.0.0.1:2379`:

```sh
./main
```

Connect to the proxy with `redis-cli`:

```sh
redis-cli -p 6380
```

Put and get key-values:

```
127.0.0.1:6380> set hello world!
OK
127.0.0.1:6380> get hello
"world!"
```
