# jepsen-dkv

Jepsen test suite for validating the DKV guarantees around
linearizability and durabiity.

Operations being validated:
- `get`
- `put`

Checkers:
- Linearizability
- Performance
- Timeline

Nemesis (failures):
- Network partition at random halves

## Usage

Assumptions:
- DKV installed at `/opt/dkv`
- DEBIAN based host
- `sudo` access
- DKV Clojure (client)[../../clients/clj/README.md] installed locally

Command:
```bash
$ cd <dkv_project_dir>/extras/jepsen_dkv
$ lein run test --nodes "node1_ip,node2_ip,node3_ip" --ssh-private-key <privake_key_path> --username <user_name> --time-limit 60
```

Alternatively, you can browse sample timelines located (here)[./results].

## License

Copyright © 2020 Apache License 2.0
