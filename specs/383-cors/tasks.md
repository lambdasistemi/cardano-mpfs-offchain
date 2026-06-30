# Tasks: CORS on the MPFS HTTP API

## Slice 1 - HTTP CORS Middleware

- [X] T383 Add HTTP CORS middleware at `mkApp`, allow `GET`/`POST`/`OPTIONS` and `content-type`, prove preflight and `GET /tokens` Origin responses with unit tests, and pass the branch gate.

## Finalization

- [ ] Audit PR body for `Closes #383` and parent `cardano-mpfs-browser#46`.
- [ ] Run final `./gate.sh`.
- [ ] Drop `gate.sh` in the final ready-for-review commit.
