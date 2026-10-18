// Concrete IndexedDB/Web Lock I/O. The synchronous Rust callback owns all
// spool decisions; its plan commits in the transaction that read the snapshot.
const DATABASE='nds-telemetry-v1', senders=new Map();
const failure=()=>new Error('telemetry_store_unavailable');
const request=operation=>new Promise((resolve,reject)=>{
  operation.onsuccess=()=>resolve(operation.result);
  operation.onerror=()=>reject(failure());
});
function open(timeout) {
  return new Promise((resolve,reject)=>{
    const operation=indexedDB.open(DATABASE,1);let abandoned=false;
    const timer=setTimeout(()=>{abandoned=true;reject(failure());},timeout);
    operation.onupgradeneeded=()=>{
      const db=operation.result,records=db.createObjectStore('records',{keyPath:'id'});
      records.createIndex('scope','scope');records.createIndex('expiry','expires_at_ms');
      records.createIndex('scope_expiry',['scope','expires_at_ms']);
      db.createObjectStore('health',{keyPath:'key'});
    };
    operation.onblocked=operation.onerror=()=>{clearTimeout(timer);abandoned=true;reject(failure());};
    operation.onsuccess=()=>{clearTimeout(timer);const db=operation.result;db.onversionchange=()=>db.close();if(abandoned)db.close();else resolve(db);};
  });
}
export function releaseSender(id) {
  const lease=senders.get(id);
  if(lease){senders.delete(id);clearTimeout(lease.timer);lease.release();}
}
export function renewSender(scope,id,timeout) {
  const lease=senders.get(id),now=performance.now();
  if(!lease?.held||lease.scope!==scope||lease.deadline<=now)return false;
  lease.deadline=Math.max(lease.deadline,now+timeout);
  clearTimeout(lease.timer);
  lease.timer=setTimeout(()=>releaseSender(id),lease.deadline-now);
  return true;
}
export function acquireSender(scope,id,timeout) {
  if(senders.has(id)||!navigator.locks)return Promise.reject(failure());
  return new Promise((resolve,reject)=>{
    let release;const held=new Promise(done=>{release=done;});
    const lease={scope,release,deadline:performance.now()+timeout};senders.set(id,lease);
    lease.timer=setTimeout(()=>{releaseSender(id);reject(failure());},timeout);
    navigator.locks.request('nds-telemetry-send:'+scope,{ifAvailable:true},async lock=>{
      if(!lock){releaseSender(id);resolve(false);return;}
      if(senders.get(id)!==lease)throw failure();
      lease.held=true;resolve(true);await held;lease.held=false;
    }).catch(()=>{releaseSender(id);reject(failure());});
  });
}
function metadata(record) {
  return {id:record.id,signal:record.signal,count:record.count,expires_at_ms:record.expires_at_ms,
    attempts:record.attempts,scope:record.scope,size:record.size,
    ...(record.inflight_until===undefined?{}:{inflight_until:record.inflight_until}),
    byte_count:record.bytes instanceof Uint8Array?record.bytes.length:null};
}
export async function queueOperation(encoded,input,policy) {
  // Copy before the first await: borrowed WASM memory may grow meanwhile.
  const bytes=new Uint8Array(input),read=JSON.parse(encoded);let db,tx,timer,done;
  try {
    db=await open(read.timeout_ms);
    tx=db.transaction(['records','health'],'readwrite',{durability:'strict'});
    done=new Promise((resolve,reject)=>{tx.oncomplete=()=>resolve();tx.onabort=tx.onerror=()=>reject(failure());});
    done.catch(()=>{});timer=setTimeout(()=>{try{tx.abort();}catch{}},read.timeout_ms);
    const records=tx.objectStore('records'),health=tx.objectStore('health'),now=Date.now();
    const [values,count,expired,rows,lookup]=await Promise.all([
      request(health.getAll(undefined,read.health_limit)),request(records.count()),
      request(records.index('expiry').getAll(IDBKeyRange.upperBound(now),read.row_limit)),
      read.scan?request(read.ordered?records.index('scope_expiry').getAll(IDBKeyRange.bound([read.scope,now],[read.scope,read.counter_limit],true,false),read.next_limit):records.index('scope').getAll(read.scope,read.row_limit)):[],
      read.lookup?request(records.get(read.lookup)):null,
    ]);
    const loaded=new Map([...expired,...rows,...(lookup?[lookup]:[])].map(row=>[row.id,row]));
    // The final IDB request's event keeps this continuation active. No await
    // separates the synchronous Rust decision from issuing its mutations.
    const plan=JSON.parse(policy(JSON.stringify({now,health:values,record_count:count,
      expired:expired.map(metadata),records:rows.map(metadata),lookup:lookup?metadata(lookup):null,
      held_scope:read.sender&&senders.get(read.sender)?.held?senders.get(read.sender).scope:null})));
    if(plan.combine.length){
      const parts=plan.combine.map(id=>loaded.get(id).bytes),joined=new Uint8Array(parts.reduce((size,part)=>size+part.length,0));
      let offset=0;for(const part of parts){joined.set(part,offset);offset+=part.length;}
      const id=plan.next.id;loaded.set(id,{...loaded.get(id),bytes:joined});
    }
    for(const id of plan.remove)records.delete(id);
    for(const row of plan.put)records.put({...loaded.get(row.id),...row});
    if(plan.append)records.add({...plan.append,bytes});
    for(const row of plan.health)health.put(row);
    if(plan.global)health.put(plan.global);
    await done;
    return plan.next?[JSON.stringify(plan.next),loaded.get(plan.next.id).bytes]:JSON.stringify(plan.result);
  } catch {
    if(tx)try{tx.abort();}catch{}
    if(done)await done.catch(()=>{});
    throw failure();
  } finally {clearTimeout(timer);db?.close();}
}
