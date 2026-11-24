//! k16 fork: Snowflake delivery tail, attached to `emit::pipeline::tail` as
//! its `snowflake` child module so the upstream file carries only the hook. See
//! FORK.md before moving code back into upstream-owned files.

use super::*;

use crate::decode::heap_decoder::HeapOp;
use crate::emit::ch_emitter::EmitterConfig;
use crate::emit::pipeline::batcher::{RoutedRow, RowChunk};
use crate::ops::oracle::Oracle;
use crate::schema::RelName;
use crate::snowflake::runtime::SnowflakeRuntime;
use crate::snowflake::types::{SnowflakeRow, TableSchema};
use std::time::Duration;
use tokio::time::Instant;

/// [`spawn_with_config`] hook: a Snowflake destination replaces the
/// ClickHouse batcher, inserter pool and resolver with its own tail
pub(super) fn snowflake_tail(
    dest: &DestEmitter,
    stats: &Arc<EmitterStats>,
    emitter_ack: &Arc<Monotone<EmitterAck>>,
    fatal: &Fatal,
    oracle: &Option<Arc<Oracle>>,
) -> Option<(mpsc::Sender<BatcherMsg>, AckHandle, TailParts)> {
    let emitter = dest.current();
    let runtime = emitter.snowflake.clone()?;
    Some(spawn_snowflake_tail(
        &emitter,
        runtime,
        stats.clone(),
        emitter_ack.clone(),
        fatal.clone(),
        oracle.clone(),
    ))
}

struct SnowflakeLookup {
    rel: Arc<crate::schema::RelDescriptor>,
    route: Arc<crate::emit::route::RouteSnapshot>,
    schema: TableSchema,
    lineage: (u64, u64),
}

struct SnowflakeBatch {
    snapshot: Option<String>,
    schema: TableSchema,
    rows: Vec<SnowflakeRow>,
    per_seq: HashMap<u64, u64>,
    permits: Vec<Arc<crate::budget::MemoryPermit>>,
    bytes: usize,
    deadline: Instant,
}

/// A placed row with the table its chunk resolved it to
struct SnowflakeRouted {
    rel: Arc<crate::schema::RelDescriptor>,
    route: Arc<crate::emit::route::RouteSnapshot>,
    row: RoutedRow,
}

enum SnowflakeTableMsg {
    /// One relation's rows of one placement chunk, in placement order,
    /// sharing that chunk's memory permit
    Rows(
        Vec<SnowflakeRouted>,
        Option<Arc<crate::budget::MemoryPermit>>,
    ),
    Flush(oneshot::Sender<()>),
}

fn spawn_snowflake_tail(
    emitter: &EmitterConfig,
    runtime: Arc<SnowflakeRuntime>,
    stats: Arc<EmitterStats>,
    emitter_ack: Arc<Monotone<EmitterAck>>,
    fatal: Fatal,
    oracle: Option<Arc<Oracle>>,
) -> (mpsc::Sender<BatcherMsg>, AckHandle, TailParts) {
    let (ack, collector) = ack::spawn(emitter_ack, fatal.clone());
    let (msg_tx, mut rx) = mpsc::channel::<BatcherMsg>(256);
    let worker_ack = ack.clone();
    let snapshot_operations = emitter.snowflake_snapshots.clone();
    let row_budget = runtime.config.batch_rows;
    let byte_budget = runtime.config.batch_bytes;
    let timeout = Duration::from_millis(runtime.config.flush_interval_ms);
    let worker = tokio::spawn(async move {
        let mut senders: HashMap<RelName, mpsc::Sender<SnowflakeTableMsg>> = HashMap::new();
        let mut workers = tokio::task::JoinSet::new();
        while let Some(msg) = rx.recv().await {
            match msg {
                BatcherMsg::Rows(chunk) => {
                    if let Err(e) = route_chunks(
                        &mut senders,
                        &mut workers,
                        &runtime,
                        &snapshot_operations,
                        &stats,
                        oracle.as_ref(),
                        &worker_ack,
                        &fatal,
                        vec![chunk],
                        (row_budget, byte_budget, timeout),
                    )
                    .await
                    {
                        fatal.set(format!("snowflake tail route: {e}"));
                        break;
                    }
                }
                BatcherMsg::Chunks(chunks) => {
                    if let Err(e) = route_chunks(
                        &mut senders,
                        &mut workers,
                        &runtime,
                        &snapshot_operations,
                        &stats,
                        oracle.as_ref(),
                        &worker_ack,
                        &fatal,
                        chunks,
                        (row_budget, byte_budget, timeout),
                    )
                    .await
                    {
                        fatal.set(format!("snowflake tail route: {e}"));
                        break;
                    }
                }
                BatcherMsg::FlushAll(reply) => {
                    let mut replies = Vec::with_capacity(senders.len());
                    let mut error = None;
                    for tx in senders.values() {
                        let (done_tx, done_rx) = oneshot::channel();
                        if tx.send(SnowflakeTableMsg::Flush(done_tx)).await.is_err() {
                            error = Some("table worker closed before flush".to_string());
                            break;
                        }
                        replies.push(done_rx);
                    }
                    if error.is_none() {
                        for result in futures::future::join_all(replies).await {
                            if result.is_err() {
                                error = Some("table worker failed during flush".into());
                                break;
                            }
                        }
                    }
                    if let Some(e) = error {
                        fatal.set(format!("snowflake tail flush: {e}"));
                        break;
                    }
                    let _ = reply.send(());
                }
            }
        }
        drop(senders);
        while let Some(result) = workers.join_next().await {
            if let Err(e) = result {
                fatal.set(format!("snowflake table worker panicked: {e}"));
            }
        }
    });
    (
        msg_tx,
        ack,
        TailParts {
            collector: AbortOnDropHandle::new(collector),
            batcher: AbortOnDropHandle::new(worker),
            resolvers: Vec::new(),
            inserters: Vec::new(),
        },
    )
}

/// Route every row of `chunks`, each sharing its chunk's memory permit
#[allow(clippy::too_many_arguments)]
async fn route_chunks(
    senders: &mut HashMap<RelName, mpsc::Sender<SnowflakeTableMsg>>,
    workers: &mut tokio::task::JoinSet<()>,
    runtime: &Arc<SnowflakeRuntime>,
    snapshot_operations: &HashMap<RelName, String>,
    stats: &Arc<EmitterStats>,
    oracle: Option<&Arc<Oracle>>,
    ack: &AckHandle,
    fatal: &Fatal,
    chunks: Vec<RowChunk>,
    (row_budget, byte_budget, timeout): (usize, usize, Duration),
) -> Result<(), String> {
    for RowChunk {
        tables,
        rows,
        permit,
    } in chunks
    {
        // One send per relation per chunk, not per row. Grouping by source
        // name keeps each relation's rows in placement order even when the
        // chunk lists it under more than one table entry
        let mut groups: Vec<(RelName, Vec<SnowflakeRouted>)> = Vec::new();
        let mut slot_of: HashMap<RelName, usize> = HashMap::new();
        for row in rows {
            let t = &tables[row.table as usize];
            let slot = *slot_of.entry(t.rel.rel_name.clone()).or_insert_with(|| {
                groups.push((t.rel.rel_name.clone(), Vec::new()));
                groups.len() - 1
            });
            groups[slot].1.push(SnowflakeRouted {
                rel: t.rel.clone(),
                route: t.route.clone(),
                row,
            });
        }
        for (rel, group) in groups {
            snowflake_route(
                senders,
                workers,
                runtime,
                snapshot_operations,
                stats,
                oracle,
                ack,
                fatal,
                rel,
                group,
                permit.clone(),
                row_budget,
                byte_budget,
                timeout,
            )
            .await?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn snowflake_route(
    senders: &mut HashMap<RelName, mpsc::Sender<SnowflakeTableMsg>>,
    workers: &mut tokio::task::JoinSet<()>,
    runtime: &Arc<SnowflakeRuntime>,
    snapshot_operations: &HashMap<RelName, String>,
    stats: &Arc<EmitterStats>,
    oracle: Option<&Arc<Oracle>>,
    ack: &AckHandle,
    fatal: &Fatal,
    key: RelName,
    rows: Vec<SnowflakeRouted>,
    chunk_permit: Option<Arc<crate::budget::MemoryPermit>>,
    row_budget: usize,
    byte_budget: usize,
    timeout: Duration,
) -> Result<(), String> {
    let tx = if let Some(tx) = senders.get(&key) {
        tx.clone()
    } else {
        let (tx, rx) = mpsc::channel(32);
        workers.spawn(snowflake_table_worker(
            runtime.clone(),
            snapshot_operations.get(&key).cloned(),
            stats.clone(),
            oracle.cloned(),
            ack.clone(),
            fatal.clone(),
            rx,
            row_budget,
            byte_budget,
            timeout,
        ));
        senders.insert(key, tx.clone());
        tx
    };
    tx.send(SnowflakeTableMsg::Rows(rows, chunk_permit))
        .await
        .map_err(|_| "Snowflake table worker closed".to_string())
}

#[allow(clippy::too_many_arguments)]
async fn snowflake_table_worker(
    runtime: Arc<SnowflakeRuntime>,
    snapshot: Option<String>,
    stats: Arc<EmitterStats>,
    oracle: Option<Arc<Oracle>>,
    ack: AckHandle,
    fatal: Fatal,
    mut rx: mpsc::Receiver<SnowflakeTableMsg>,
    row_budget: usize,
    byte_budget: usize,
    timeout: Duration,
) {
    let mut batch: Option<SnowflakeBatch> = None;
    let mut lookup: Option<SnowflakeLookup> = None;
    let mut deliveries = tokio::task::JoinSet::new();
    let max_parallel = runtime.config.channels_per_table;
    loop {
        let next = batch.as_ref().map(|b| b.deadline);
        tokio::select! {
            msg=rx.recv()=>match msg {
                Some(SnowflakeTableMsg::Rows(rows,permit))=>{
                    let mut failed=false;
                    for row in rows {
                        if let Err(e)=snowflake_add(&runtime,snapshot.as_deref(),oracle.as_ref(),&mut batch,&mut lookup,row,permit.as_ref(),timeout).await {
                            fatal.set(format!("snowflake table: {e}"));failed=true;break;
                        }
                        if batch.as_ref().is_some_and(|b| b.rows.len() >= row_budget || b.bytes >= byte_budget)
                            && let Err(e)=snowflake_start_delivery(&runtime,&stats,&ack,&mut batch,&mut deliveries,max_parallel).await {
                                fatal.set(format!("snowflake table delivery: {e}"));failed=true;break;
                            }
                    }
                    if failed {break;}
                }
                Some(SnowflakeTableMsg::Flush(reply))=>{
                    let result=async {
                        snowflake_start_delivery(&runtime,&stats,&ack,&mut batch,&mut deliveries,max_parallel).await?;
                        snowflake_drain_deliveries(&mut deliveries).await
                    }.await;
                    if let Err(e)=result {fatal.set(format!("snowflake table flush: {e}"));break;}
                    let _=reply.send(());
                }
                None=>{
                    let result=async {
                        snowflake_start_delivery(&runtime,&stats,&ack,&mut batch,&mut deliveries,max_parallel).await?;
                        snowflake_drain_deliveries(&mut deliveries).await
                    }.await;
                    if let Err(e)=result {fatal.set(format!("snowflake table final flush: {e}"));}
                    break;
                }
            },
            _=async {if let Some(at)=next {tokio::time::sleep_until(at).await}},if next.is_some()=>{
                if let Err(e)=snowflake_start_delivery(&runtime,&stats,&ack,&mut batch,&mut deliveries,max_parallel).await {fatal.set(format!("snowflake table deadline: {e}"));break;}
            },
            result=deliveries.join_next(),if !deliveries.is_empty()=>{
                if let Err(e)=snowflake_delivery_result(result) {fatal.set(format!("snowflake table delivery: {e}"));break;}
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn snowflake_add(
    runtime: &SnowflakeRuntime,
    snapshot: Option<&str>,
    oracle: Option<&Arc<Oracle>>,
    batch: &mut Option<SnowflakeBatch>,
    lookup: &mut Option<SnowflakeLookup>,
    routed: SnowflakeRouted,
    chunk_permit: Option<&Arc<crate::budget::MemoryPermit>>,
    timeout: Duration,
) -> Result<(), String> {
    let SnowflakeRouted {
        rel,
        route,
        mut row,
    } = routed;
    if !lookup
        .as_ref()
        .is_some_and(|cached| Arc::ptr_eq(&cached.rel, &rel) && Arc::ptr_eq(&cached.route, &route))
    {
        let schema = runtime
            .schema_for(&rel, &route)
            .await
            .map_err(|e| e.to_string())?;
        let mut lineage = runtime.lineage(&rel).map_err(|e| e.to_string())?;
        if let Some(operation) = snapshot {
            lineage.1 = runtime
                .state
                .generation(operation)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| "missing snapshot generation".to_owned())?
                .generation_id;
        }
        *lookup = Some(SnowflakeLookup {
            rel: rel.clone(),
            route: route.clone(),
            schema,
            lineage,
        });
    }
    let cached = lookup.as_ref().expect("lookup populated");
    let schema = cached.schema.clone();
    let (incarnation, generation) = cached.lineage;
    if let Some(oracle) = oracle {
        crate::snowflake::types::render_source_text(oracle, &mut row.committed, &rel)
            .await
            .map_err(|e| e.to_string())?;
    }
    let encoded = match row.committed.decoded.op {
        HeapOp::Insert => vec![SnowflakeRow::from_committed_with_lineage(
            &schema,
            &row.committed,
            0,
            false,
            &runtime.source_identity,
            incarnation,
            generation,
        )?],
        HeapOp::Update | HeapOp::HotUpdate => SnowflakeRow::update_rows_with_lineage(
            &schema,
            &row.committed,
            0,
            &runtime.source_identity,
            incarnation,
            generation,
        )?,
        HeapOp::Delete => vec![SnowflakeRow::from_committed_with_lineage(
            &schema,
            &row.committed,
            0,
            true,
            &runtime.source_identity,
            incarnation,
            generation,
        )?],
        HeapOp::Truncate => return Err("TRUNCATE requires a Snowflake generation barrier".into()),
    };
    let bytes = row.committed.decoded.approx_bytes();
    let held = batch.get_or_insert_with(|| SnowflakeBatch {
        snapshot: snapshot.map(str::to_owned),
        schema: schema.clone(),
        rows: Vec::new(),
        per_seq: HashMap::new(),
        permits: Vec::new(),
        bytes: 0,
        deadline: Instant::now() + timeout,
    });
    if held.schema != schema {
        return Err("Snowflake schema changed inside open batch".into());
    }
    held.rows.extend(encoded);
    *held.per_seq.entry(row.seq).or_insert(0) += 1;
    held.bytes = held.bytes.saturating_add(bytes);
    if let Some(p) = row.value_permit {
        held.permits.push(p);
    }
    // A chunk's rows arrive together: hold its permit once per batch
    if let Some(p) = chunk_permit
        && !held.permits.last().is_some_and(|last| Arc::ptr_eq(last, p))
    {
        held.permits.push(p.clone());
    }
    Ok(())
}
async fn snowflake_start_delivery(
    runtime: &Arc<SnowflakeRuntime>,
    stats: &Arc<EmitterStats>,
    ack: &AckHandle,
    batch: &mut Option<SnowflakeBatch>,
    deliveries: &mut tokio::task::JoinSet<Result<(), String>>,
    max_parallel: usize,
) -> Result<(), String> {
    if batch.is_none() {
        return Ok(());
    }
    while deliveries.len() >= max_parallel {
        snowflake_delivery_result(deliveries.join_next().await)?;
    }
    let Some(held) = batch.take() else {
        return Ok(());
    };
    let runtime = runtime.clone();
    let ack = ack.clone();
    let stats = stats.clone();
    deliveries.spawn(async move {
        complete_after_delivery(
            async {
                let n_rows = held.rows.len() as u64;
                let started = std::time::Instant::now();
                match &held.snapshot {
                    Some(operation) => runtime
                        .deliver_snapshot(held.schema, held.rows, operation)
                        .await
                        .map(|_| ()),
                    None => runtime.deliver(held.schema, held.rows).await,
                }?;
                stats
                    .rows_emitted
                    .fetch_add(n_rows, std::sync::atomic::Ordering::Relaxed);
                stats
                    .blocks_sent
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                stats.inserter_ch_nanos.fetch_add(
                    started.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                Ok(())
            },
            &ack,
            held.per_seq.into_iter().collect(),
            held.permits,
        )
        .await
    });
    Ok(())
}
fn snowflake_delivery_result(
    result: Option<Result<Result<(), String>, tokio::task::JoinError>>,
) -> Result<(), String> {
    match result {
        Some(Ok(result)) => result,
        Some(Err(e)) => Err(format!("delivery task failed: {e}")),
        None => Err("delivery task disappeared".into()),
    }
}
async fn snowflake_drain_deliveries(
    deliveries: &mut tokio::task::JoinSet<Result<(), String>>,
) -> Result<(), String> {
    while !deliveries.is_empty() {
        snowflake_delivery_result(deliveries.join_next().await)?;
    }
    Ok(())
}

async fn complete_after_delivery<F>(
    delivery: F,
    ack: &AckHandle,
    per_seq: Vec<(u64, u64)>,
    permits: Vec<Arc<crate::budget::MemoryPermit>>,
) -> Result<(), String>
where
    F: std::future::Future<Output = anyhow::Result<()>>,
{
    delivery.await.map_err(|e| e.to_string())?;
    ack.delivered(per_seq);
    drop(permits);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn snowflake_delivery_failure_does_not_advance_ack() {
        let watermark = Arc::new(Monotone::<EmitterAck>::new(crate::pos::Pos::ZERO));
        let (ack, collector) = ack::spawn(watermark.clone(), Fatal::new());
        let mut seqs = ack.seqs(0);
        let (first, second) = (
            seqs.open(100, ack::Publish::Commit),
            seqs.open(200, ack::Publish::Commit),
        );
        seqs.place(first, 1);
        seqs.place(second, 1);
        let failed = complete_after_delivery(
            async { anyhow::bail!("warehouse rejected batch") },
            &ack,
            vec![(0, 1)],
            vec![],
        )
        .await;
        assert!(failed.is_err());
        complete_after_delivery(async { Ok(()) }, &ack, vec![(1, 1)], vec![])
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), ack.wait_through(2))
                .await
                .is_err()
        );
        complete_after_delivery(async { Ok(()) }, &ack, vec![(0, 1)], vec![])
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), ack.wait_through(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(watermark.get(), 200);
        drop((ack, seqs));
        collector.await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_snowflake_batches_ack_contiguously() {
        let watermark = Arc::new(Monotone::<EmitterAck>::new(crate::pos::Pos::ZERO));
        let (ack, collector) = ack::spawn(watermark.clone(), Fatal::new());
        let mut seqs = ack.seqs(0);
        let (first, second) = (
            seqs.open(100, ack::Publish::Commit),
            seqs.open(200, ack::Publish::Commit),
        );
        seqs.place(first, 1);
        seqs.place(second, 1);
        let (first_tx, first_rx) = oneshot::channel::<()>();
        let (second_tx, second_rx) = oneshot::channel::<()>();
        let mut deliveries = tokio::task::JoinSet::new();
        for (seq, ready) in [(0, first_rx), (1, second_rx)] {
            let ack = ack.clone();
            deliveries.spawn(async move {
                complete_after_delivery(
                    async move { ready.await.map_err(anyhow::Error::from) },
                    &ack,
                    vec![(seq, 1)],
                    vec![],
                )
                .await
            });
        }
        second_tx.send(()).unwrap();
        snowflake_delivery_result(deliveries.join_next().await).unwrap();
        assert_eq!(watermark.get(), 0, "later batch cannot cross the gap");
        first_tx.send(()).unwrap();
        snowflake_drain_deliveries(&mut deliveries).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), ack.wait_through(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(watermark.get(), 200);
        drop((ack, seqs));
        collector.await.unwrap();
    }
}
