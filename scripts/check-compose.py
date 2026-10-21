#!/usr/bin/env python3
"""Isolated real pipeline acceptance. All accounts, credentials and events are owned
runtime input. No provider accounts, machine inventory or evidence enter the tree.
"""
import argparse
import base64
import datetime
import concurrent.futures
import io
import json
import os
import pathlib
import secrets
import socket
import subprocess
import tarfile
import tempfile
import time
import tomllib
import re
import urllib.error
import urllib.request
import uuid

ROOT=pathlib.Path(__file__).resolve().parents[1]
TARGET=pathlib.Path(json.loads(subprocess.check_output(
    ['cargo','metadata','--locked','--no-deps','--format-version','1'],
    cwd=ROOT,text=True,timeout=30))['target_directory'])
SERVER_LOCK=tomllib.loads((ROOT/'acceptance-server.lock').read_text())
assert SERVER_LOCK['source_repo']=='NDDev-Technocracy-OpenNetwork-NDS/nddev-device-sync-server'
SERVER_SOURCE=SERVER_LOCK['source_commit']
assert re.fullmatch('[a-f0-9]{40}',SERVER_SOURCE)
SERVER_IMAGE='nds-server-acceptance:'+SERVER_SOURCE[:12]
DEBIAN='debian:trixie-slim@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f'
PG='postgres:18.6-bookworm@sha256:afc7e2d441324c0388fa80c3d24f733b4194a4eb7f47dd8ee2b08eb1a24a647c'

class CommandFailure(AssertionError):
    def __init__(self,summary,diagnostics):
        super().__init__(summary);self.diagnostics=diagnostics

def command(*args,check=True,**kw):
    result=subprocess.run(args,capture_output=True,text=True,**kw)
    if check and result.returncode:
        # Build/config diagnostics contain only public source. Runtime logs are
        # never emitted here; inspect only safe failure classes in owned tests.
        raise CommandFailure('command failed: '+str(args[0])+' '+str(args[1])+' exit '+str(result.returncode),result.stderr[-8000:])
    return result

def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0));return sock.getsockname()[1]

PROBE=None

def request(url,data=None,token=None,headers=None,method=None):
    headers=dict(headers or {})
    if token:headers['Authorization']='Bearer '+token
    if isinstance(data,dict):data=json.dumps(data);headers['Content-Type']='application/json'
    if isinstance(data,bytes):data=data.decode()
    lines=['url = '+json.dumps(url), 'noproxy = "*"','max-time = 12','silent','show-error','write-out = "\\n%{http_code}"']
    if method:lines.append('request = '+json.dumps(method))
    for name,value in headers.items():lines.append('header = '+json.dumps(name+': '+value))
    if data is not None:lines.append('data-binary = '+json.dumps(data))
    result=command('docker','exec','-i',PROBE,'curl','--config','-',input='\n'.join(lines)+'\n',check=False,timeout=15)
    if result.returncode:raise ConnectionError('internal test transport unavailable')
    body,code=result.stdout.rsplit('\n',1)
    return int(code),body.encode()

def eventually(predicate,seconds=45):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        try:
            result=predicate()
            if result:return result
        except (urllib.error.URLError,ConnectionError,TimeoutError):pass
        time.sleep(.2)
    raise AssertionError('bounded acceptance condition timed out')

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture-receipt',type=pathlib.Path)
    parser.add_argument('--fixture-seconds',type=int,default=1200)
    args=parser.parse_args()
    if not 60 <= args.fixture_seconds <= 3600: parser.error('fixture lifetime must be 60..3600 seconds')
    if args.fixture_receipt is not None and (not args.fixture_receipt.is_absolute() or args.fixture_receipt.resolve().is_relative_to(ROOT) or args.fixture_receipt.exists()): parser.error('fixture receipt must be a new absolute path outside source')
    assert not command('git','status','--porcelain',cwd=ROOT).stdout.strip(), 'commit collector changes before source-qualified integration'
    obs_source=command('git','rev-parse','HEAD',cwd=ROOT).stdout.strip()
    assert re.fullmatch('[a-f0-9]{40}',obs_source)
    obs_image='nds-observability-acceptance:'+obs_source[:12]
    command('cargo','build','--locked','-p','nddev-device-sync-telemetry','--example','sdk_probe',cwd=ROOT,timeout=180)
    source_names=['nddev-device-sync-server','nddev-device-sync-observability','sdk-acceptance']
    client_events=[]
    client_log=os.environ.get('NDS_CLIENT_EVENTS_FILE')
    if client_log:
        client_log=pathlib.Path(client_log)
        assert client_log.is_absolute() and client_log.stat().st_size<=32*1024*1024
        for line in client_log.read_text().splitlines():
            start=line.find('{')
            if start<0:continue
            try:event,_=json.JSONDecoder().raw_decode(line[start:])
            except json.JSONDecodeError:continue
            if isinstance(event,dict) and event.get('service.name')=='nddev-device-sync-client':
                client_events.append(event)
                assert len(client_events)<=256,'client acceptance event bound'
        assert any(event.get('event.name')=='client.failure' for event in client_events),'actual client failure required'
        source_names.append('nddev-device-sync-client')
    # Docker 28.2 introduced the finite Fluent write timeout. Refuse an older
    # engine before building images or creating any ephemeral runtime state.
    version=command('docker','version','--format','{{.Server.Version}}',timeout=10).stdout.strip()
    parts=re.match(r'^(\d+)\.(\d+)\.(\d+)',version)
    assert parts and tuple(map(int,parts.groups())) >= (28,2,0), 'Docker Engine >=28.2.0 is required for bounded Fluent writes'
    project='nds-obs-test-'+secrets.token_hex(5)
    with tempfile.TemporaryDirectory(prefix='nds-obs-acceptance-') as temp:
        directory=pathlib.Path(temp);private=directory/'secrets';private.mkdir(mode=0o700)
        values={key:secrets.token_urlsafe(32) for key in ['postgres_admin_password','postgres_migrator_password','postgres_runtime_password','ingest_token','admin_token','vector_password','oo_password']}
        values['oo_password']+='Aa1!'
        values['oo_user']=secrets.token_hex(12)+'@example.invalid'
        for key,role in [('migration_url','nds_migrator'),('runtime_url','nds_runtime')]:
            password=values['postgres_migrator_password' if key=='migration_url' else 'postgres_runtime_password']
            values[key]=f'postgres://{role}:{password}@postgres:5432/nds_observability'
            values['server_'+key]=f'postgres://{role}:{password}@server-postgres:5432/nds'
        for key,value in values.items():
            path=private/key;path.write_text(value+'\n');path.chmod(0o444)
        env=os.environ.copy();env.update(NDS_TEST_SECRETS=str(private),NDS_FLUENT_PORT=str(free_port()),NDS_OBS_IMAGE=obs_image,NDS_OO_USER=values['oo_user'],NDS_OO_PASSWORD=values['oo_password'],NDS_SOURCE_STALE_SECONDS='5')
        server_dir=directory/'server';server_dir.mkdir()
        archive=subprocess.check_output(['git','-C',os.environ.get('NDS_SERVER_SOURCE_DIR',str(ROOT.parent/'nddev-device-sync-server')),'archive',SERVER_SOURCE])
        with tarfile.open(fileobj=io.BytesIO(archive)) as tree:tree.extractall(server_dir,filter='data')
        if command('docker','image','inspect',SERVER_IMAGE,check=False).returncode:
            command('docker','build','--tag',SERVER_IMAGE,'--build-arg','NDS_SOURCE_COMMIT='+SERVER_SOURCE,str(server_dir),timeout=900)
        obs_dir=directory/'observability';obs_dir.mkdir()
        archive=subprocess.check_output(['git','-C',str(ROOT),'archive',obs_source])
        with tarfile.open(fileobj=io.BytesIO(archive)) as tree:tree.extractall(obs_dir,filter='data')
        command('docker','build','--tag',obs_image,'--build-arg','NDS_SOURCE_COMMIT='+obs_source,str(obs_dir),timeout=900)
        override={'services':{
            'observability':{'environment':{'NDS_EXPECTED_SOURCES':','.join(source_names)}},
            'probe':{'image':obs_image,'entrypoint':['sleep','infinity'],'read_only':True,'cap_drop':['ALL'],'security_opt':['no-new-privileges:true'],'mem_limit':'32m','pids_limit':16,'networks':['pipeline']},
            'server-postgres':{'image':PG,'user':'postgres','read_only':True,'cap_drop':['ALL'],'security_opt':['no-new-privileges:true'],'mem_limit':'512m','pids_limit':128,'environment':{'POSTGRES_DB':'nds','POSTGRES_PASSWORD_FILE':'/run/secrets/postgres_admin_password'},'secrets':['postgres_admin_password','postgres_migrator_password','postgres_runtime_password'],'volumes':[f'{server_dir}/deploy/postgres-init.sh:/docker-entrypoint-initdb.d/10-roles.sh:ro'],'tmpfs':['/var/lib/postgresql:uid=999,gid=999,mode=0700,size=256m','/var/run/postgresql:uid=999,gid=999,mode=3775,size=16m','/tmp:uid=999,gid=999,mode=1777,size=16m'],'networks':['pipeline'],'healthcheck':{'test':['CMD-SHELL','pg_isready -U postgres -d nds -h 127.0.0.1'],'interval':'1s','timeout':'2s','retries':30}},
            'server-migrate':{'image':SERVER_IMAGE,'command':['migrate'],'mem_limit':'256m','read_only':True,'cap_drop':['ALL'],'security_opt':['no-new-privileges:true'],'environment':{'NDS_MIGRATION_DATABASE_URL_FILE':'/run/secrets/server_migration_url'},'secrets':['server_migration_url'],'networks':['pipeline'],'depends_on':{'server-postgres':{'condition':'service_healthy'}}},
            'server':{'image':SERVER_IMAGE,'command':['serve'],'read_only':True,'cap_drop':['ALL'],'security_opt':['no-new-privileges:true'],'mem_limit':'512m','pids_limit':64,'environment':{'DATABASE_URL_FILE':'/run/secrets/server_runtime_url','NDS_SERVER_ADDR':'0.0.0.0:8080','NDS_MAX_REQUESTS':'1','NDS_MAX_CONNECTIONS':'4'},'secrets':['server_runtime_url'],'networks':['pipeline'],'depends_on':{'server-migrate':{'condition':'service_completed_successfully'}},'logging':{'driver':'fluentd','options':{'fluentd-address':'127.0.0.1:'+env['NDS_FLUENT_PORT'],'fluentd-async':'true','fluentd-buffer-limit':'256','fluentd-max-retries':'3','fluentd-retry-wait':'1s','fluentd-write-timeout':'2s','fluentd-async-reconnect-interval':'1s','mode':'non-blocking','max-buffer-size':'1m','cache-max-size':'2m','cache-max-file':'2'}}},
        },'secrets':{'server_migration_url':{'file':str(private/'server_migration_url')},'server_runtime_url':{'file':str(private/'server_runtime_url')}}}
        if args.fixture_receipt is not None:
            fixture_ports={name:free_port() for name in ['observability','vector','openobserve']}
            for name,port in [('observability',8080),('vector',4318),('openobserve',5080)]:
                override['services'].setdefault(name,{})['ports']=[f'127.0.0.1:{fixture_ports[name]}:{port}']
                override['services'][name]['networks']=['pipeline','collector']
            env['NDS_SOURCE_STALE_SECONDS']='90'
        override_path=directory/'override.json';override_path.write_text(json.dumps(override))
        prefix=['docker','compose','--project-name',project,'--file',str(ROOT/'deploy/compose.yaml'),'--file',str(override_path)]
        def compose(*args,**kw):return command(*prefix,*args,env=env,**kw)
        def service_id(service):return compose('ps','--quiet',service).stdout.strip()
        def sql(service,text):return command('docker','exec',service_id(service),'psql','-XAt','-v','ON_ERROR_STOP=1','-U','postgres','-d','nds' if service=='server-postgres' else 'nds_observability','-c',text).stdout.strip()
        def endpoint(service,port):return f'http://{service}:{port}'
        try:
            # Only this uniquely named, test-owned volume requires initial UID setup.
            command('docker','volume','create',project+'_oo_data')
            command('docker','run','--rm','--network','none','--user','0','--cap-drop','ALL','--cap-add','CHOWN','--security-opt','no-new-privileges:true','--mount',f'type=volume,src={project}_oo_data,dst=/data',DEBIAN,'chown','10001:10001','/data')
            compose('pull','openobserve','vector','postgres','server-postgres',timeout=900)
            compose('up','--detach','openobserve','vector','postgres','server-postgres','probe',timeout=120)
            global PROBE
            PROBE=service_id('probe')
            oo=endpoint('openobserve',5080)
            eventually(lambda:request(oo+'/healthz')[0]==200)
            oo_headers={'Authorization':'Basic '+base64.b64encode((values['oo_user']+':'+values['oo_password']).encode()).decode()}
            code,_=request(oo+'/api/default/streams',headers=oo_headers)
            assert code==200,f'OpenObserve basic API status {code}'
            compose('up','--detach','observability','server',timeout=120)
            obs=endpoint('observability',8080);server=endpoint('server',8080)
            eventually(lambda:request(obs+'/v1/ready')[0]==200)
            eventually(lambda:request(server+'/v1/ready')[0]==200)
            migration_events=[json.loads(line) for line in compose('logs','--no-log-prefix','--no-color','migrate').stdout.splitlines() if line.startswith('{')]
            completed=[event for event in migration_events if event.get('event.name')=='migration.completed']
            assert len(completed)==1, 'one actual migration completion required'
            assert completed[0]['migration.version']==int(sql('postgres',"SELECT value FROM nds_observability_meta WHERE key='schema'")), 'migration telemetry must identify the actual schema'
            assert completed[0]['standards.release']==tomllib.loads((ROOT/'standarts.lock').read_text())['source_release'], 'telemetry standards must match the consumed lock'
            assert completed[0]['source.commit']==obs_source, 'migration telemetry must identify the actual collector source'
            if args.fixture_receipt is not None:
                # These are generated fixture credentials, never operator files.
                for name in ['ingest_token','admin_token']:
                    path=directory/(name+'-host');path.write_text(values[name]);path.chmod(0o600)
                gateway=directory/'gateway.json'
                gateway.write_text(json.dumps({'origin':f"http://127.0.0.1:{fixture_ports['observability']}", 'trace_origin':f"http://127.0.0.1:{fixture_ports['vector']}", 'ingest_token_file':str(directory/'ingest_token-host'), 'admin_token_file':str(directory/'admin_token-host')}));gateway.chmod(0o600)
                query=directory/'query.json';query.write_text(json.dumps({'origin':f"http://127.0.0.1:{fixture_ports['openobserve']}",'user':values['oo_user'],'password':values['oo_password']}));query.chmod(0o600)
                receipt=args.fixture_receipt;stop=receipt.with_suffix('.stop')
                state={'status':'running','gateway_config':str(gateway),'query_config':str(query),'project':project,'stop_marker':str(stop),'expires_at_unix':int(time.time())+args.fixture_seconds,'sources':{'observability':obs_source,'server':SERVER_SOURCE}}
                with receipt.open('x') as handle: json.dump(state,handle)
                receipt.chmod(0o600)
                print('Isolated observability fixture ready; private receipt written outside source.',flush=True)
                deadline=time.monotonic()+args.fixture_seconds
                try:
                    while time.monotonic()<deadline and not stop.exists():
                        if request(obs+'/v1/ready')[0]!=200: raise AssertionError('owned collector became unavailable')
                        time.sleep(1)
                finally:
                    state['status']='stopped';receipt.write_text(json.dumps(state))
                return

            def status():
                code,body=request(obs+'/v2/observability/status',token=values['admin_token']);assert code==200;return json.loads(body)
            eventually(lambda:status()['delivered_events']>0)
            trace=uuid.uuid4().hex
            assert request(server+'/v1/health',headers={'traceparent':'00-'+trace+'-'+secrets.token_hex(8)+'-01'})[0]==200
            def search(table,kind,where):
                assert table.replace('_','').isalnum()
                now=time.time_ns()//1000
                payload={'query':{'sql':f'SELECT * FROM "{table}" WHERE {where}','start_time':now-86_400_000_000,'end_time':now+2_000_000,'from':0,'size':100}}
                code,body=request(oo+'/api/default/_search?type='+kind,payload,headers=oo_headers)
                if code!=200:return []
                return json.loads(body).get('hits',[])
            logs=eventually(lambda:search('nds','logs',"trace_id='"+trace+"'"))
            assert any(row.get('source_commit',row.get('source.commit'))==SERVER_SOURCE for row in logs)
            collector_logs=eventually(lambda:search('nds','logs',"service_name='nddev-device-sync-observability' AND event_name='server.started'"))
            assert any(row.get('source_commit',row.get('source.commit'))==obs_source for row in collector_logs)
            print('Searchable source identities verified: collector '+obs_source+'; server '+SERVER_SOURCE)
            def streams(kind):
                code,body=request(oo+'/api/default/streams?type='+kind,headers=oo_headers)
                assert code==200
                return [row['name'] for row in json.loads(body)['list']]
            trace_streams=eventually(lambda:streams('traces'))
            spans=eventually(lambda:next((found for table in trace_streams if (found:=search(table,'traces',"trace_id='"+trace+"'"))),None))
            assert spans
            metric_streams=eventually(lambda:streams('metrics'))
            assert any(name.startswith('nds_observability_events_accepted') for name in metric_streams)
            eventually(lambda:status()['metrics']['successes']>0 and status()['traces']['successes']>0)
            print('Actual server trace-correlated log searchable; native processing OTLP span and operational metrics searchable')
            def incidents():
                code,body=request(obs+'/v2/observability/incidents',token=values['admin_token']);assert code==200;return json.loads(body)['incidents']
            # The records come from the actual shared Rust producer, not hand-
            # assembled successful payloads. Ingress -> Vector -> OpenObserve
            # and the durable incident machine consume exactly those records.
            produced=command(TARGET/'debug/examples/sdk_probe','--client-cycle',timeout=5)
            assert len(produced.stdout.encode())<=128*1024
            cycle=[json.loads(line) for line in produced.stdout.splitlines()]
            ready_index=next(i for i,event in enumerate(cycle) if event['event.name']=='client.ready')
            def ingest(events):
                data=''.join(json.dumps(event,separators=(',',':'))+'\n' for event in events)
                code,reply=request(obs+'/v2/observability/events',data,token=values['ingest_token'],headers={'Content-Type':'application/x-ndjson'})
                assert code==200
                return json.loads(reply)
            first=cycle[:ready_index]
            assert ingest(first)['accepted']==len(first)
            failure=next(event for event in first if event['event.name']=='client.failure')
            actual=eventually(lambda:next((row for row in incidents() if row['service_name']=='sdk-acceptance' and row['kind']=='process' and row['state']=='open'),None))
            assert actual['occurrences']==1
            assert ingest(first)['duplicates']==len(first)
            assert ingest(cycle[ready_index:])['accepted']==len(cycle)-ready_index
            eventually(lambda:any(row['incident_id']==actual['incident_id'] and row['state']=='resolved' for row in incidents()))
            observed=eventually(lambda:search('nds','logs',"trace_id='"+failure['trace_id']+"'"))
            assert any(row.get('error_type',row.get('error.type'))=='flutter_framework_error' for row in observed)
            print('Actual shared SDK client failure searchable: one incident, duplicate replay retained, readiness resolved it')
            audit_output=command(TARGET/'debug/examples/sdk_probe',timeout=5).stdout
            assert len(audit_output.encode())<=128*1024
            audit=next(event for event in map(json.loads,audit_output.splitlines()) if event['event.name']=='company.command.completed')
            assert ingest([audit])['accepted']==1
            assert ingest([audit])['duplicates']==1
            indexed=eventually(lambda:search('nds','logs',"trace_id='"+audit['trace_id']+"'"))
            record=next(row for row in indexed if row.get('event_name',row.get('event.name'))=='company.command.completed')
            for field in ['operation.id','target.user.id','invitation.id','auth_method']:
                assert record.get(field.replace('.','_'),record.get(field))==audit[field], 'indexed audit field differs: '+field
            print('Actual SDK company audit and passkey method survive canonical ingress, replay deduplication, Vector and OpenObserve search')

            # The real SDK's frames pass the same context enrichment boundary
            # supplied by an authenticated gateway, not an always-on source.
            import hashlib
            device=str(uuid.uuid4())
            device_source='nddev-device-sync-client.'+hashlib.sha256(device.encode()).hexdigest()[:24]
            device_cycle=[event | {'service.name':'nddev-device-sync-client','device.id':device,'producer.instance_id':str(uuid.uuid5(uuid.NAMESPACE_OID,event['producer.instance_id']+device))} for event in cycle]
            assert ingest(device_cycle[:ready_index])['accepted']==ready_index
            device_incident=eventually(lambda:next((row for row in incidents() if row['service_name']==device_source and row['kind']=='process' and row['state']=='open'),None))
            assert ingest(device_cycle[:ready_index])['duplicates']==ready_index
            assert ingest(device_cycle[ready_index:])['accepted']==len(device_cycle)-ready_index
            eventually(lambda:any(row['incident_id']==device_incident['incident_id'] and row['state']=='resolved' for row in incidents()))
            assert device_source not in {source['service_name'] for source in status()['sources']}
            assert sql('postgres',"SELECT count(*) FROM client_producers WHERE service_name='"+device_source+"'")== '1'
            print('Actual SDK client stream: device-scoped incident, producer replay/sequence and optional liveness passed')

            # Offline expiry-prioritized batches can deliver a later producer
            # record first. Preserve both actual SDK records without rewinding
            # source progress, hiding late failures or weakening replay.
            late_device=str(uuid.uuid4())
            late_instance=str(uuid.uuid4())
            late_source='nddev-device-sync-client.'+hashlib.sha256(late_device.encode()).hexdigest()[:24]
            late_cycle=[event | {'service.name':'nddev-device-sync-client','device.id':late_device,'producer.instance_id':late_instance} for event in cycle]
            earlier=next(event for event in late_cycle if event['event.name']=='client.failure')
            later=next(event for event in late_cycle if event['event.name']=='client.ready')
            assert earlier['producer.sequence']<later['producer.sequence']
            assert ingest([later])['accepted']==1
            freshness=sql('postgres',"SELECT last_event_at::text FROM sources WHERE service_name='"+late_source+"'")
            assert ingest([earlier])['accepted']==1
            delayed_incident=eventually(lambda:next((row for row in incidents() if row['service_name']==late_source and row['kind']=='process' and row['state']=='open'),None))
            assert delayed_incident['occurrences']==1
            assert ingest([earlier])['duplicates']==1
            assert next(row for row in incidents() if row['incident_id']==delayed_incident['incident_id'])['occurrences']==1
            assert sql('postgres',"SELECT last_sequence FROM client_producers WHERE instance_id='"+late_instance+"'::uuid")==str(later['producer.sequence'])
            assert sql('postgres',"SELECT last_event_at::text FROM sources WHERE service_name='"+late_source+"'")==freshness
            indexed=eventually(lambda:found if len(found:=search('nds','logs',"device_id='"+late_device+"'"))==2 else None)
            assert {row.get('event_name',row.get('event.name')) for row in indexed}=={'client.failure','client.ready'}
            # Rebinding is still rejected transactionally, including its source
            # row and the fingerprint inserted before producer validation.
            rebound=later | {'service.name':'nddev-device-sync-agent','producer.sequence':later['producer.sequence']+1}
            code,_=request(obs+'/v2/observability/events',json.dumps(rebound)+'\n',token=values['ingest_token'],headers={'Content-Type':'application/x-ndjson'})
            assert code==400
            assert sql('postgres',"SELECT count(*) FROM sources WHERE service_name='nddev-device-sync-agent."+hashlib.sha256(late_device.encode()).hexdigest()[:24]+"'")=='0'
            assert sql('postgres',"SELECT last_sequence FROM client_producers WHERE instance_id='"+late_instance+"'::uuid")==str(later['producer.sequence'])
            recovered=command(TARGET/'debug/examples/sdk_probe','--client-cycle',timeout=5)
            recovery=next(event for line in recovered.stdout.splitlines() if (event:=json.loads(line))['event.name']=='client.ready')
            recovery=recovery | {'service.name':'nddev-device-sync-client','device.id':late_device,'producer.instance_id':str(uuid.uuid5(uuid.NAMESPACE_OID,recovery['producer.instance_id']+late_device))}
            assert ingest([recovery])['accepted']==1
            eventually(lambda:any(row['incident_id']==delayed_incident['incident_id'] and row['state']=='resolved' for row in incidents()))
            print('Delayed SDK records indexed once; high-water/freshness retained; late failure observed and later recovery resolved it; rebinding rejected')

            if client_events:
                for start in range(0,len(client_events),100):ingest(client_events[start:start+100])
                for kind in ['flutter_framework_error','dart_unhandled_error']:
                    event=next(event for event in client_events if event.get('error.type')==kind)
                    observed=eventually(lambda:search('nds','logs',"trace_id='"+event['trace_id']+"'"))
                    assert any(row.get('error_type',row.get('error.type'))==kind for row in observed)
                print('Actual Flutter framework and root-isolate async failures searchable through canonical ingress, Vector and OpenObserve')
            lock=subprocess.Popen(['docker','exec',service_id('server-postgres'),'psql','-XAt','-U','postgres','-d','nds','-c',"BEGIN; LOCK TABLE nddev_schema_meta IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(8); COMMIT;"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            eventually(lambda:sql('server-postgres',"SELECT count(*) FROM pg_locks WHERE relation='nddev_schema_meta'::regclass AND mode='AccessExclusiveLock' AND granted")=='1')
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                held=pool.submit(request,server+'/v1/ready')
                eventually(lambda:sql('server-postgres',"SELECT count(*) FROM pg_stat_activity WHERE usename='nds_runtime' AND wait_event_type='Lock'")=='1')
                for _ in range(3):assert request(server+'/v1/health')[0]==503
                incident=eventually(lambda:next((row for row in incidents() if row['kind']=='admission' and row['state']=='open' and row['occurrences']>=2),None),seconds=5)
                assert incident['owner']=='operator' and incident['severity']=='warning'
                path=obs+'/v2/observability/incidents/'+incident['incident_id']
                body={'operation_id':str(uuid.uuid7()),'expected_revision':incident['revision']}
                headers={'X-NDS-Actor':'operator'}
                assert request(path+'/acknowledge',body,headers=headers)[0]==401
                assert request(path+'/acknowledge',body,token=values['ingest_token'],headers=headers)[0]==401
                code,reply=request(path+'/acknowledge',body,token=values['admin_token'],headers=headers);assert code==200
                acknowledged=json.loads(reply);assert acknowledged['state']=='acknowledged'
                assert request(path+'/acknowledge',body,token=values['admin_token'],headers=headers)==(200,reply)
                conflicting=body|{'expected_revision':acknowledged['revision']}
                assert request(path+'/acknowledge',conflicting,token=values['admin_token'],headers=headers)[0]==409
                until=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat()
                code,silenced=request(path+'/silence',{'operation_id':str(uuid.uuid7()),'expected_revision':acknowledged['revision'],'until':until},token=values['admin_token'],headers=headers);assert code==200
                assert 'silenced_until' in json.loads(silenced)
                lock.wait(timeout=12);held.result(timeout=12)
            assert request(server+'/v1/ready')[0]==200
            resolved=eventually(lambda:next((row for row in incidents() if row['incident_id']==incident['incident_id'] and row['state']=='resolved'),None))
            assert 'silenced_until' not in resolved
            compose('restart','observability',timeout=30)
            eventually(lambda:request(obs+'/v1/ready')[0]==200)
            assert request(path+'/acknowledge',body,token=values['admin_token'],headers=headers)==(200,reply)
            print('Actual PostgreSQL pressure: incident dedup, denied callers, acknowledgement, silence, recovery and persisted idempotent replay passed')
            compose('stop','--timeout','10','openobserve',timeout=20)
            assert request(server+'/v1/health')[0]==200
            eventually(lambda:status()['queued_events']>0 and status()['logs']['state']=='degraded')
            assert status()['queued_events']<=4096 and status()['queued_bytes']<=67108864
            compose('start','openobserve')
            eventually(lambda:request(oo+'/healthz')[0]==200)
            eventually(lambda:status()['queued_events']==0 and status()['logs']['state']=='healthy',seconds=60)
            print('Real OpenObserve outage/restart: bounded durable log outbox resumed delivery')
            # Collector failure happens before durable ingress: zero outbox
            # alone must not report that the source stream is fresh.
            assert request(server+'/v1/health')[0]==200
            eventually(lambda:status()['queued_events']==0)
            compose('stop','--timeout','10','vector',timeout=20)
            assert request(server+'/v1/health')[0]==200
            eventually(lambda:any(row['service_name']=='nddev-device-sync-server' and row['state']=='stale' for row in status()['sources']),seconds=15)
            compose('start','vector')
            def recovered_source():
                request(server+'/v1/health')
                return any(row['service_name']=='nddev-device-sync-server' and row['state']=='fresh' for row in status()['sources'])
            eventually(recovered_source,seconds=30)
            print('Collector interruption: independent source staleness detected and recovered; no exact loss count inferred from gaps')
            override['services']['observability']['environment'].update(NDS_TELEMETRY_ENABLED='false',NDS_VECTOR_PASSWORD_FILE='/not-configured')
            override_path.write_text(json.dumps(override))
            compose('up','--detach','--no-deps','--force-recreate','observability',timeout=30)
            eventually(lambda:request(obs+'/v1/ready')[0]==200)
            disabled=status();assert disabled['export_mode']=='disabled'
            assert all(disabled[k]['state']=='disabled' for k in ['logs','metrics','traces'])
            disabled_trace=uuid.uuid4().hex
            assert request(server+'/v1/health',headers={'traceparent':'00-'+disabled_trace+'-'+secrets.token_hex(8)+'-01'})[0]==200
            assert request(obs+'/v2/observability/incidents')[0]==401
            time.sleep(4)
            assert not search('nds','logs',"trace_id='"+disabled_trace+"'")
            assert not any(search(table,'traces',"trace_id='"+disabled_trace+"'") for table in trace_streams)
            local=compose('logs','--tail','30','observability').stdout
            assert 'ingress.rejected' in local and 'server.started' in local
            after_disabled=status()
            assert after_disabled['queued_events']<=disabled['queued_events']
            assert after_disabled['delivered_events']==disabled['delivered_events']
            print('Full export opt-out: no new logs/spans exported, exporter credential unnecessary, local security/lifecycle signals retained')
            override['services']['observability']={'environment':{'NDS_EXPECTED_SOURCES':','.join(source_names)}}
            override_path.write_text(json.dumps(override))
            compose('up','--detach','--no-deps','--force-recreate','observability',timeout=30)
            eventually(lambda:request(obs+'/v1/ready')[0]==200)
            ids=[service_id(name) for name in ['postgres','observability','vector','openobserve','server','server-postgres']]
            samples=command('docker','stats','--no-stream','--format','{{json .}}',*ids).stdout
            total=0.0
            for line in samples.splitlines():
                used=json.loads(line)['MemUsage'].split('/')[0].strip()
                for unit,factor in [('GiB',1024),('MiB',1),('KiB',1/1024),('B',1/1048576)]:
                    if used.endswith(unit):total+=float(used[:-len(unit)])*factor;break
            assert total<6144
            print(f'Sampled Docker memory working set including separate server test DB: {total:.1f} MiB; configured runtime caps below 8 GiB')
            retained=','.join(['nddev-device-sync-server']+[f'acceptance-source-{i}' for i in range(16-len(source_names))])
            registration_name=project+'-registration'
            compose('run','--detach','--rm','--no-deps','--name',registration_name,'--env','NDS_EXPECTED_SOURCES='+retained,'--env','NDS_TELEMETRY_ENABLED=false','observability')
            try:
                eventually(lambda:sql('postgres','SELECT count(*) FROM sources WHERE required')=='16')
            finally:command('docker','stop','--time','15',registration_name,check=False,timeout=20)
            compose('restart','observability',timeout=30)
            eventually(lambda:request(obs+'/v1/ready')[0]==200)
            sources=status()['sources'];assert len(sources)==16 and sum(row['state']=='inactive' for row in sources)==16-len(source_names)
            rejected=compose('run','--rm','--no-deps','--env','NDS_EXPECTED_SOURCES=unregistered-source','--env','NDS_TELEMETRY_ENABLED=false','observability',check=False,timeout=20)
            assert rejected.returncode!=0
            assert sql('postgres','SELECT count(*) FROM sources WHERE required')=='16'
            print('Real PostgreSQL source registration: canonical set capped at16, retained inactive rows visible after restart,17th source refused transactionally')
            sql('postgres',"UPDATE delivery_state SET time_floor=clock_timestamp()+interval '2 hours'")
            eventually(lambda:sql('postgres','SELECT count(*) FROM incident_actions')=='0')
            compose('restart','observability',timeout=30)
            eventually(lambda:request(obs+'/v1/ready')[0]==200)
            assert request(path+'/acknowledge',body,token=values['admin_token'],headers=headers)[0]==400
            print('Persisted time floor: expired receipt pruned, service restarted, old UUIDv7 refused after observed-time rollback')
        except Exception as error:
            diagnostics=getattr(error,"diagnostics","")+"\n"+compose("logs","--tail","100","openobserve","vector","migrate","observability",check=False).stdout
            for private_value in sorted(values.values(),key=len,reverse=True):diagnostics=diagnostics.replace(private_value,"[generated-private-value]")
            print("\n".join(line for line in diagnostics.splitlines() if any(word in line for word in [" ERROR "," WARN ","panicked", "Error response from daemon"]))[:9000])
            raise
        finally:
            # Scope is the random Compose project created above, never other work.
            compose('down','--volumes','--remove-orphans','--timeout','20',check=False,timeout=60)
            command('docker','volume','rm',project+'_oo_data',check=False)

if __name__=='__main__':main()
