#!/usr/bin/env python3
"""Reproduce consumed schemas and Rust DTOs from one immutable protocol pin."""
import argparse
from pathlib import Path
import subprocess
import tempfile
from protocol_source import ROOT,source

arguments=argparse.ArgumentParser(description=__doc__)
arguments.add_argument('--write',action='store_true')
options=arguments.parse_args()
SCHEMAS={
    'contracts/v2/telemetry-event.schema.json':['crates/sdk/telemetry-event.schema.json'],
    'contracts/v2/observability.schema.json':['contracts/observability.schema.json'],
    'contracts/v2/control-plane.schema.json':['contracts/control-plane.schema.json'],
}
FILES=['package.json','bun.lock','scripts/generate-dtos.mjs','contracts/v1/source-response.schema.json',*SCHEMAS]

def project(path,data):
    target=ROOT/path
    if options.write:target.write_bytes(data)
    else:assert target.read_bytes()==data,'canonical protocol projection drift: '+path

with tempfile.TemporaryDirectory(prefix='nds-observability-codegen-') as temporary:
    directory=Path(temporary)
    for name in FILES:
        data=source(name);path=directory/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
        for target in SCHEMAS.get(name,[]):project(target,data)
    subprocess.run(['bun','install','--frozen-lockfile','--ignore-scripts'],cwd=directory,check=True,timeout=120,stdout=subprocess.DEVNULL)
    for profile,name in [('telemetry','crates/sdk/src/event.rs'),('observability','src/wire.rs'),('source','src/source_wire.rs')]:
        output=directory/name
        output.parent.mkdir(parents=True,exist_ok=True)
        subprocess.run(['node','scripts/generate-dtos.mjs','rust',profile,str(output)],cwd=directory,check=True,timeout=30)
        subprocess.run(['rustfmt','--edition','2024',str(output)],cwd=ROOT,check=True,timeout=20)
        project(name,output.read_bytes())
print('Canonical observability schemas and DTOs projected' if options.write else 'Canonical observability schemas and DTO drift check passed')
