#!/usr/bin/env python

import subprocess
import pathlib
import os
import time
import shutil
import re

def mkdirp(path):
  pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def start_master(f, n):
  return subprocess.Popen(['./main'],
    stdout=f,
    stderr=subprocess.STDOUT,
    stdin=subprocess.PIPE,
    cwd=os.getcwd(),
    env={
      'COORDINATOR': '1',
      'RPC_PORT': '3001',
      'NODE_ID': '1',
      'NUM_NODES': str(n)
    })

def start_replica(f, n):
  i = (n + 1) + 1 # node 1 is master, node 2 is first replica
  return subprocess.Popen(['./main'],
    stdout=f,
    stderr=subprocess.STDOUT,
    cwd=os.getcwd(),
    env={
      'RPC_PORT': f'300{i}',
      'NODE_ID': str(i),
    })

def start_client(reqs, f, master, replica_count):

  commands = []
  for i in range(reqs):
    commands.append('BEGIN')
    for j in range(replica_count):
      commands.append(f'SET {chr(65+j)}.{"a"*(i+1)} {i+1}')
    commands.append('COMMIT')
  
  # s = ''.join(f'BEGIN\nSET A.{"a"*(i+1)} {i+1}\nCOMMIT\n' for i in range(reqs)).encode('utf-8')
  s = ''.join(c + '\n' for c in commands).encode('utf-8')
  print('about to communicate')
  # stdout =
  master.communicate(input=s)
  print('ok')
  # [0].decode('utf-8')
  # print('got stdout')
  # f.write(stdout)

def clean(replica_count):
  shutil.rmtree('out', ignore_errors=True)
  mkdirp('out')

def build():
  subprocess.check_call(['go', 'build', 'main.go'],
    stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

def run_experiment(reqs, replica_count):
  replica_files = []
  replica_processes = []
  with open('out/master.log', 'w') as master_f, open('out/client.log', 'w') as client_f:
    master_process = start_master(master_f, replica_count)
    for i in range(replica_count):
      fr = open(f'out/replica{i}.log', 'w')
      replica_files.append(fr)
      replica_processes.append(start_replica(fr, i))

    print('waiting for processes to start')
    time.sleep(5)
    start_client(reqs, client_f, master_process, replica_count)
    print('client terminated')

    for f in replica_files:
      f.close()
    for p in replica_processes:
      p.terminate()
      p.wait()

    master_process.terminate()
    master_process.wait()

def collect_data(replica_count):

  monitor_time = 0
  with open('out/master.log', 'r') as f:
    for m in re.finditer(r'Total time taken: (\d+)', f.read()):
      client_time = int(m.group(1))
      print('client time', client_time)
    for m in re.finditer(r'Monitor time taken: (\d+)', f.read()):
      t = int(m.group(1))
      monitor_time += t
      print('master time', t)

  for i in range(replica_count):
    with open(f'out/replica{i}.log', 'r') as f:
      for m in re.finditer(r'Monitor time taken: (\d+)', f.read()):
        t = int(m.group(1))
        monitor_time += t
        print('replica', i, 'time', t)

  print('monitor time', monitor_time)
  return monitor_time, client_time

def run_it_all(reqs, runs, replica_count):
  monitor_time = 0
  client_time = 0
  for i in range(runs):
    print(f'---- run {i}')
    clean(replica_count)
    run_experiment(reqs, replica_count)
    m, c = collect_data(replica_count)
    monitor_time += m
    client_time += c

  monitor_time /= runs
  client_time /= runs
  print(f'------')
  overhead = monitor_time / client_time
  print(f'avg overhead for {replica_count} replicas: {overhead}')


if __name__ == "__main__":
  build()

  # testing baby steps
  # runs = 1
  # runs = 2
  # replica_counts = {2}
  # replica_counts = {3}
  # reqs = 1
  # reqs = 2
  # reqs = 5

  # paper configuration
  runs = 5
  replica_counts = {2, 4, 6}
  reqs = 100
  for c in replica_counts:
    print(f'------ {c} replicas')
    run_it_all(reqs, runs, c)
