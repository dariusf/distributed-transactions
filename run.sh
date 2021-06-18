#!/bin/bash

set -x
set -e

p1='RPC_PORT=3002 NODE_ID=2 ./main'
p2='RPC_PORT=3003 NODE_ID=3 ./main'
c='COORDINATOR=1 RPC_PORT=3001 NODE_ID=1 NUM_NODES=2 ./main'

# cmd=$(printf "BEGIN\nSET A.a 1\nCOMMIT\n")

go build main.go

tmux kill-session -t test_2pc || true

# https://stackoverflow.com/a/40009032

# ------------
# |    p1    |
# |----------|
# |  p2  | c |
# ------------

tmux new-session -d -s test_2pc $SHELL \; \
  send-keys "$p1" ENTER \; \
  split-window -v \; \
  send-keys "$p2" ENTER \; \
  split-window -h \; \
  send-keys "$c" ENTER

delayed_input() {
  sleep 5
  # tmux send-keys "$cmd" ENTER
  tmux send-keys BEGIN ENTER
  sleep 1
  tmux send-keys 'SET A.a 1' ENTER
  sleep 1
  tmux send-keys COMMIT ENTER
}

(delayed_input &)

tmux a
