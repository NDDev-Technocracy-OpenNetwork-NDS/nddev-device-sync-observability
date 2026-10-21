#!/usr/bin/env python3
"""Exercise actual concurrent process stdout, without retaining runtime events."""
import os
import json
import pathlib
import subprocess
ROOT=pathlib.Path(__file__).resolve().parents[1]
TARGET=pathlib.Path(json.loads(subprocess.check_output(
    ['cargo','metadata','--locked','--no-deps','--format-version','1'],
    cwd=ROOT,text=True,timeout=30))['target_directory'])
subprocess.run(['cargo','build','--locked','-p','nddev-device-sync-telemetry','--example','sdk_probe'],cwd=ROOT,check=True)
result=subprocess.run([TARGET/'debug/examples/sdk_probe'],capture_output=True,check=True,timeout=5)
assert len(result.stdout)<128*1024
records=[json.loads(line) for line in result.stdout.splitlines()]
assert len(records)==48
audit=next(event for event in records if event['event.name']=='company.command.completed')
assert audit['operation.id']=='01900000-0000-7000-8000-000000000001'
assert audit['user.id']=='actor-1' and audit['target.user.id']=='member-2'
assert audit['invitation.id']=='A'*43 and audit['tenant.id']=='company-1'
assert audit['module']=='organizations' and audit['scope']=='database' and audit['auth_method']=='passkey'
assert len(audit['trace_id'])==32 and len(audit['span_id'])==16

assert [event['producer.sequence'] for event in records]==list(range(1,49))
assert len({event['producer.instance_id'] for event in records})==1
assert sum(event['event.name']=='logging.debug.disabled' and event['reason']=='expired' for event in records)==1
assert not any(event['event.name']=='diagnostic.after_expiry' for event in records)
operations=[event for event in records if event['event.name']=='operation.completed']
assert len(operations)==32
assert len({event['trace_id'] for event in operations})==8
assert all(len(event['span_id'])==16 for event in operations)
assert all(len(line)<=16384 for line in result.stdout.splitlines())
assert not result.stderr
failures=[event for event in records if event['event.name']=='client.failure']
assert {event['error.type'] for event in failures}=={'flutter_framework_error','dart_unhandled_error','native_bridge_error'}
assert all(event['severity']=='error' and len(event['trace_id'])==32 and len(event['span_id'])==16 for event in failures)
assert sum(event['event.name']=='process.started' for event in records)==1
assert sum(event['event.name']=='process.stopped' for event in records)==1
print('SDK actual process: ordered canonical events, typed client failures, real traces, lifecycle and autonomous debug expiry passed')

# Producers race the terminal close against a real, actively drained pipe.
# Every attempted record is observed or explicitly counted as lost.
race=subprocess.run([TARGET/'debug/examples/sdk_probe','--shutdown-race'],capture_output=True,check=True,timeout=10)
assert len(race.stdout)<16*1024*1024
evidence=json.loads(race.stderr)
received=[json.loads(line) for line in race.stdout.splitlines()]
assert len(received)+evidence['dropped']==evidence['attempted']
assert evidence['pending']==0
assert sum(event['event.name']=='process.stopped' for event in received)<=1
print('SDK concurrent shutdown: all attempted records delivered locally or counted lost, zero pending, idempotent close')

panic=subprocess.run([TARGET/'debug/examples/sdk_probe','--panic'],capture_output=True,timeout=5)
assert panic.returncode != 0 and not panic.stderr
assert b'SYNTHETIC_PRIVATE_PANIC' not in panic.stdout and b'/private/example' not in panic.stdout
failed=[json.loads(line) for line in panic.stdout.splitlines()]
assert sum(event.get('error.type')=='rust_panic' and event['severity']=='fatal' for event in failed)==1
print('SDK real Rust panic: canonical fatal class, no raw payload/stack/default panic output')

# This pipe deliberately remains undrained until after the child exits. The
# worker may block in the OS; caller work and Logger::drop must still finish.
blocked=subprocess.Popen([TARGET/'debug/examples/sdk_probe','--blocked-stdout'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
try:
    assert blocked.wait(timeout=6)==0
    evidence=json.loads(blocked.stderr.read(4096))
    assert evidence['dropped']>0 and evidence['shutdown_ms']<2500 and evidence['terminated']
    print('SDK undrained stdout: bounded work/shutdown, explicit queue loss, post-shutdown writes rejected')
finally:
    if blocked.poll() is None:blocked.kill();blocked.wait(timeout=2)
    blocked.stdout.close();blocked.stderr.close()

cli=subprocess.run([TARGET/'debug/examples/sdk_probe','--stderr'],capture_output=True,check=True,timeout=5)
assert json.loads(cli.stdout)=={'result':'ok'}
assert len(cli.stderr)<128*1024
cli_events=[json.loads(line) for line in cli.stderr.splitlines()]
assert any(event['event.name']=='client.ready' for event in cli_events)
assert any(event['event.name']=='process.stopped' for event in cli_events)
print('SDK CLI output: one stdout result, canonical lifecycle diagnostics on bounded stderr')

if os.name == 'posix':
    unavailable=subprocess.run([TARGET/'debug/examples/sdk_probe','--closed-stdout-panic'],capture_output=True,timeout=5)
    assert unavailable.returncode != 0
    assert not unavailable.stdout and not unavailable.stderr
    print('SDK unavailable sink: subsequent real panic exposes neither payload nor stack')
