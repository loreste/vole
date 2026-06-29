# Installing Vole

Vole compiles to two binaries: `vole` (the server) and `vole-cli` (the command-line client). There's nothing else to install -- no runtime, no shared libraries, no config files you need to create first.

## From source

You need Go 1.22 or later. If you're not sure what you have, run `go version`.

```bash
git clone https://github.com/your-org/vole.git
cd vole
go build -o vole ./cmd/vole
go build -o vole-cli ./cmd/vole-cli
```

That gives you two binaries in the current directory. Move them wherever you keep your tools:

```bash
sudo mv vole vole-cli /usr/local/bin/
```

Or put them in `~/bin`, or `/opt/vole/bin`, or wherever makes sense for your setup. They're static binaries with no dependencies, so any location works.

## Verify it works

Start the server:

```bash
vole
```

You should see something like:

```
2025/06/29 12:00:00 vole listening on 127.0.0.1:7379 node=... slots=0-16383
```

In another terminal, try talking to it:

```bash
vole-cli PING
```

If you get `PONG` back, you're in business.

## Running as a service

Vole doesn't ship with systemd units or init scripts -- every environment is a little different, and a templated service file tends to cause more confusion than it prevents. Here's a starting point for systemd:

```ini
[Unit]
Description=Vole data store
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/vole \
    --addr 0.0.0.0:7379 \
    --appendonly \
    --appendfilename /var/lib/vole/vole.aof \
    --snapshot /var/lib/vole/vole.snap \
    --snapshot-interval 5m
Restart=on-failure
RestartSec=5
User=vole
Group=vole
WorkingDirectory=/var/lib/vole
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

Save that as `/etc/systemd/system/vole.service`, then:

```bash
sudo useradd --system --no-create-home vole
sudo mkdir -p /var/lib/vole
sudo chown vole:vole /var/lib/vole
sudo systemctl daemon-reload
sudo systemctl enable --now vole
```

Check that it came up:

```bash
sudo systemctl status vole
vole-cli PING
```

## Docker

There's no official Docker image yet, but building one is straightforward:

```dockerfile
FROM golang:1.22 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -o /vole ./cmd/vole
RUN CGO_ENABLED=0 go build -o /vole-cli ./cmd/vole-cli

FROM scratch
COPY --from=build /vole /vole
COPY --from=build /vole-cli /vole-cli
EXPOSE 7379 8080
ENTRYPOINT ["/vole"]
CMD ["--addr", "0.0.0.0:7379"]
```

```bash
docker build -t vole .
docker run -p 7379:7379 vole
```

If you want persistence to survive container restarts, mount a volume:

```bash
docker run -p 7379:7379 \
  -v vole-data:/data \
  vole --addr 0.0.0.0:7379 \
    --appendonly --appendfilename /data/vole.aof \
    --snapshot /data/vole.snap --snapshot-interval 5m
```

## What about Windows?

Go cross-compiles without any fuss:

```bash
GOOS=windows GOARCH=amd64 go build -o vole.exe ./cmd/vole
GOOS=windows GOARCH=amd64 go build -o vole-cli.exe ./cmd/vole-cli
```

We haven't tested extensively on Windows, but the networking and file I/O are all standard library, so it should work. If you run into something, open an issue.

## Upgrading

Vole's on-disk formats (AOF and snapshots) are designed to be forward-compatible. To upgrade:

1. Stop the running server (Ctrl-C or `systemctl stop vole`). It'll save a final snapshot and flush the AOF on the way out.
2. Replace the binary.
3. Start it again.

Your data files don't need to change. If we ever make a breaking format change, we'll call it out in the release notes and provide a migration path.

## Uninstalling

Delete the binaries and your data directory. There's nothing else to clean up -- no config files scattered across your system, no background daemons left behind.

```bash
sudo rm /usr/local/bin/vole /usr/local/bin/vole-cli
sudo rm -rf /var/lib/vole  # if you used the systemd setup above
```
