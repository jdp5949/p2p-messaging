# Deploying the relay

Server Go is too old to build there, so cross-compile on your Mac:

```sh
GOOS=linux GOARCH=amd64 go build -o relay-linux ./cmd/relay
```

Ship it (alias `ec2` = ssh to the Oracle VM `ubuntu@129.153.24.33`):

```sh
ssh ec2 'mkdir -p ~/p2p-relay'
scp -i ~/ssh-key-2025-03-20.key relay-linux ubuntu@129.153.24.33:~/p2p-relay/relay
```

Grant the `ubuntu` user read access to the nip.io cert + install the service:

```sh
ssh ec2 'sudo setfacl -R -m u:ubuntu:rX /etc/letsencrypt/live /etc/letsencrypt/archive'
scp -i ~/ssh-key-2025-03-20.key deploy/p2p-relay.service ubuntu@129.153.24.33:/tmp/
ssh ec2 'sudo mv /tmp/p2p-relay.service /etc/systemd/system/ \
  && sudo systemctl daemon-reload \
  && sudo systemctl enable --now p2p-relay \
  && sudo systemctl status p2p-relay --no-pager'
```

Verify it listens on 9009:

```sh
ssh ec2 'sudo ss -tlnp | grep 9009'
```

If Oracle Cloud's VCN security list does not already allow TCP 9009, add an
ingress rule for `0.0.0.0/0 -> 9009` in the OCI console. (The host iptables
already permits 9009.)

## Local / self-hosted (no TLS)

```sh
go build -o relay ./cmd/relay
./relay -addr :9009
```

Point peers at it with `p2p -relay your.host:9009 send`.
