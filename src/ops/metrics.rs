//! HTTP/OpenMetrics metrics surface, plus the probes an orchestrator needs.
//!
//! `/metrics` over plain TCP, encoded by `prometheus-client` as
//! [OpenMetrics text](https://github.com/prometheus/OpenMetrics/blob/v1.0.0/specification/OpenMetrics.md#text-format):
//! counter samples carry `_total`, the body closes with `# EOF`.
//!
//! Declarations live here rather than in a mutable metric registry: values are
//! already atomics elsewhere in the pipeline, so a scrape encodes one owned
//! [`MetricsSnapshot`] through a [`Collector`].
//!
//! Registry is `Arc`-cloneable: daemon's main loop writes at status-tick
//! cadence, HTTP server reads a snapshot per request. Endpoint is read-only by
//! design (no `/quit`, no admin verbs); operator actions stay on the CLI.
//!
//! Paths, all `GET` or `HEAD`; anything else is 404, or 405 on a known path:
//!
//! - `/metrics`: the scrape.
//! - `/healthz`: liveness, 200 whenever the daemon's runtime answers. A TCP
//!   check cannot tell that much: the kernel completes the handshake from the
//!   listen backlog even when the process is wedged. Pipeline progress stays
//!   out of it on purpose: a long bootstrap, an initial load or a destination
//!   outage is something to alert on, not to restart over.
//! - `/readyz`: 200 once bootstrap is done and the session streams, while the
//!   shadow postmaster is up; 503 naming the first unmet condition otherwise.
//!   A destination outage leaves it ready. See [`Health`].
//! - `/status`: the control socket's `status`, as JSON. Off unless the daemon
//!   hands [`serve`] a control context, because it names databases, tables
//!   and destinations on an unauthenticated port.

use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use prometheus_client::collector::Collector;
use prometheus_client::encoding::{
    DescriptorEncoder, EncodeCounterValue, EncodeGaugeValue, GaugeValueEncoder, MetricEncoder,
    NoLabelSet, text,
};
use prometheus_client::metrics::MetricType;
use prometheus_client::registry::Registry;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::catalog::pending::DegradeReason;
use crate::decode::heap_decoder::HEAP_OP_LABELS;
use crate::ops::bridge::{OP_COUNT, OP_LABELS};
use crate::pos::{Drain, EmitterAck, FilterDispatched, Floor, Pos, ShadowReplay, SourceReceived};
use crate::source::transition::SWITCH_FAILURE_REASONS;

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("bind {addr}: {source}")]
    Bind { addr: String, source: io::Error },
}

macro_rules! encode_metric {
    ($enc:expr, counter, $name:expr, $help:expr, $value:expr) => {
        counter($enc, $name, $help, $value)?
    };
    ($enc:expr, gauge, $name:expr, $help:expr, $value:expr) => {
        gauge($enc, $name, $help, $value)?
    };
    // LSN 0 is InvalidXLogRecPtr: the position is not known yet, eg the
    // default snapshot served from process start until the session's first
    // status write. A 0 sample between real LSNs reads as a ~80 TB jump to
    // deriv() and lag queries, so the sample is left out instead
    ($enc:expr, lsn, $name:expr, $help:expr, $value:expr) => {
        if $value.get() != 0 {
            gauge($enc, $name, $help, $value)?
        }
    };
    ($enc:expr, counter, $name:expr, $help:expr, $value:expr, $key:literal, $labels:expr) => {
        counter_series($enc, $name, $help, $key, $labels.into_iter().zip($value))?
    };
    ($enc:expr, gauge, $name:expr, $help:expr, $value:expr, $key:literal, $labels:expr) => {
        gauge_series($enc, $name, $help, $key, $labels.into_iter().zip($value))?
    };
}

/// [`encode_metric`] for a family every database carries: the `database=`
/// label comes first, a declared label array second
macro_rules! encode_db_metric {
    ($enc:expr, $dbs:expr, $names:expr, counter, $name:expr, $help:expr, $field:ident) => {
        counter_series(
            $enc,
            $name,
            $help,
            "database",
            $names
                .iter()
                .zip($dbs)
                .map(|(db, s)| (db.as_ref(), s.$field)),
        )?
    };
    ($enc:expr, $dbs:expr, $names:expr, gauge, $name:expr, $help:expr, $field:ident) => {
        gauge_series(
            $enc,
            $name,
            $help,
            "database",
            $names
                .iter()
                .zip($dbs)
                .map(|(db, s)| (db.as_ref(), s.$field)),
        )?
    };
    (
        $enc:expr, $dbs:expr, $names:expr, counter, $name:expr, $help:expr, $field:ident,
        $key:literal, $labels:expr
    ) => {
        counter_grid(
            $enc,
            $name,
            $help,
            $key,
            $names
                .iter()
                .zip($dbs)
                .map(|(db, s)| (db.as_ref(), $labels.into_iter().zip(s.$field))),
        )?
    };
    (
        $enc:expr, $dbs:expr, $names:expr, gauge, $name:expr, $help:expr, $field:ident,
        $key:literal, $labels:expr
    ) => {
        gauge_grid(
            $enc,
            $name,
            $help,
            $key,
            $names
                .iter()
                .zip($dbs)
                .map(|(db, s)| (db.as_ref(), $labels.into_iter().zip(s.$field))),
        )?
    };
}

/// Declares [`MetricsSnapshot`] and the families a scrape renders off it, so a
/// metric is one entry rather than a field, a doc comment and an encode call.
/// Help text doubles as the field's documentation; `walshadow_` plus the field
/// name is the family name, which [`declare`] trims of the `_by_<label>` tail a
/// `["label" = LABELS]` array carries, and of the `_total` OpenMetrics re-adds
/// to counter samples.
///
/// A `custom` field the table declares but does not encode: either its family
/// carries several labels, a bare sample beside a labelled one, or a value no
/// field holds, so [`encode_snapshot`] writes it by hand, or `ctl status` is
/// its only reader.
macro_rules! snapshot {
    (
        custom { $($(#[doc = $custom_doc:literal])* $custom:ident: $custom_ty:ty,)* }
        $(
            $(#[doc = $doc:literal])*
            $kind:ident $field:ident: $ty:ty $([$key:literal = $labels:expr])? = $help:literal,
        )*
    ) => {
        /// Snapshot of every value `/metrics` renders. Daemon writes per
        /// status-line iteration; HTTP readers take a read lock and serialise.
        #[derive(Debug, Default, Clone)]
        pub struct MetricsSnapshot {
            $($(#[doc = $custom_doc])* pub $custom: $custom_ty,)*
            $(
                #[doc = $help]
                $(#[doc = $doc])*
                pub $field: $ty,
            )*
        }

        fn encode_fields(snap: &MetricsSnapshot, enc: &mut DescriptorEncoder<'_>) -> fmt::Result {
            $(encode_metric!(
                enc,
                $kind,
                concat!("walshadow_", stringify!($field)),
                $help,
                snap.$field
                $(, $key, $labels)?
            );)*
            Ok(())
        }
    };
}

snapshot! {
    custom {
        /// One entry per configured tenant, rendered `tenant=` labelled
        tenants: Vec<TenantMetrics>,
        /// `route` is `"to_shadow"` / `"to_decoder"`
        records_by_rm_route: BTreeMap<(String, &'static str), u64>,
        /// Raw-stash records `[dirty, marker]` x op, rendered `kind=`/`op=`
        /// labelled
        raw_stash_records_by_kind_op: [[u64; 7]; 2],
        /// Commit-resolve raw decode records `[toast, ordinary]` x op
        raw_decode_records_by_kind_op: [[u64; 7]; 2],
        /// Per-database series, config order. Every family [`DbSeries`]
        /// declares renders one `database=` labelled sample from each entry
        by_database: Vec<DbSeries>,
        /// `initial_load` backfills recorded in the ledger but not yet
        /// complete (in flight, or awaiting re-run on next boot), summed
        /// across databases. `ctl status` reads it; the scrape renders the
        /// per-database split instead
        config_backfills_pending: u64,
        unmapped_rows_by_table: Vec<(String, String, u64)>,
        rows_inserted_by_table: Vec<(String, String, u64)>,
        cells_inserted_by_type: Vec<(String, &'static str, u64)>,
        skipped_records_by_rel: Vec<SkippedRel>,
        /// Refusal a crossing parked on, rendered as `crossing_wedged`. The
        /// pump keeps publishing rather than exiting into a restart that
        /// re-crosses and re-fails, so this is how the daemon says it is
        /// waiting for an operator
        crossing_blocked_on: &'static str,
        /// PostgreSQL source system identifier, rendered as a `source_info`
        /// label to hold 64 bits a float64 sample would round
        source_system_id: u64,
        /// Proof the last endpoint swap failed on, from the crossing's own
        /// vocabulary; empty once one lands
        source_endpoint_swap_blocked_on: &'static str,
        /// `crossing_blocked_on`'s own words, which name the pair behind it
        crossing_detail: String,
        /// Both frozen pause numbers were re-derived by this process from a
        /// pause it found already in effect, so a pair read before a restart
        /// is stale
        pause_refrozen: bool,
        /// Every term of the promotion gate holds, so the target may be
        /// promoted
        promotion_ready: bool,
        /// First term that does not, empty once ready
        promotion_blocked_on: &'static str,
        promotion_target_in_recovery: bool,
        /// Target's `pg_last_wal_replay_lsn()`, read while paused
        promotion_target_replay_lsn: u64,
        /// Target's `pg_last_wal_receive_lsn()`, read while paused
        promotion_target_receive_lsn: u64,
    }

    lsn source_received_lsn: Pos<SourceReceived> =
        "Source PG's most recent server_wal_end seen on the replication socket.",
    /// Last segment-boundary LSN dispatched downstream; becomes durable once
    /// segment fsync lands
    lsn filter_lsn: Pos<FilterDispatched> =
        "LSN of the last filtered WAL byte the daemon has dispatched.",
    lsn shadow_replay_lsn: Pos<ShadowReplay> =
        "Shadow PG's pg_last_wal_replay_lsn(), polled at status cadence.",
    lsn decoder_commit_lsn: Pos<Drain> = "Highest commit LSN drained out of the xact buffer.",
    /// Live insert watermark, unlike manifest's resume-safe `emitter_ack`
    lsn emitter_ack_lsn: Pos<EmitterAck> = "Contiguous-done watermark from the insert pipeline.",
    lsn floor_lsn: Pos<Floor> = "Durable resume floor: restart resumes here and pruners cut to it.",
    gauge xact_active: u64 = "Active transactions buffered in memory or on spill.",
    gauge xact_bytes_in_memory: u64 = "Bytes held in memory across all buffered xacts.",
    gauge spill_xacts_active: u64 = "Xacts with at least one entry currently in their spill file.",
    gauge spill_bytes_active: u64 = "Bytes currently held across all active xact spill files.",
    gauge drain_resident_bytes: u64 =
        "Bytes resident inside an active commit drain (heads + chunk generations + mirror rows).",
    gauge drain_chunk_resident_bytes: u64 =
        "Chunk-generation share of drain_resident_bytes, held until consumers drop.",
    gauge drain_row_resident_bytes: u64 =
        "Mirror-row share of drain_resident_bytes, held until store put completes.",
    gauge toast_xact_spool_bytes: u64 =
        "Bytes in transaction TOAST body spool files (disk, not resident).",
    gauge resident_payload_bytes: u64 =
        "Bytes held by live memory-budget permits across pipeline stages.",
    gauge resident_payload_peak_bytes: u64 = "High-water mark of resident payload permit bytes.",
    counter memory_budget_waits_total: u64 = "Budget acquisitions that waited for a release.",
    counter memory_budget_overshoots_total: u64 =
        "Requests above a budget compartment, admitted with only the satisfiable share metered.",
    counter memory_budget_big_leaf_waits_total: u64 =
        "Large values that waited for another large value to finish.",
    gauge bootstrap_deferred_bytes: u64 =
        "Resident bytes in the in-memory prefixes of every bootstrap TOAST-deferred spool.",
    gauge bootstrap_deferred_spool_bytes: u64 =
        "Encoded bytes in every bootstrap TOAST-deferred spool file.",
    gauge bootstrap_deferred_replay_bytes: u64 =
        "Total encoded file bytes in active deferred TOAST replays, excluding spool headers.",
    gauge bootstrap_deferred_replayed_bytes: u64 =
        "Encoded file bytes processed by active deferred TOAST replays; excludes prefetched batches and does not imply ClickHouse acknowledgement.",
    counter pending_rows_total: u64 =
        "Undecided backup rows written to ClickHouse pending tables.",
    counter pending_tables_total: u64 = "Pending tables created for undecided backup rows.",
    counter pending_tables_dropped_total: u64 =
        "Pending tables dropped after all transaction outcomes were resolved.",
    counter pending_xacts_settled_total: u64 =
        "Transaction outcomes resolved for pending rows.",
    gauge pending_outstanding_xids: u64 =
        "Transaction ids pending rows still wait on.",
    gauge pending_undecidable_xids: u64 =
        "Outstanding transaction ids absent from shadow pg_xact, leaving rows pending.",
    /// Bootstrap pump stage attribution. Live while a greenfield bootstrap
    /// runs, then frozen at its final values for the rest of the session
    gauge bootstrap_parts_total: u64 =
        "Total tar parts in the object-store base backup; 0 for the direct source.",
    counter bootstrap_parts_done: u64 =
        "Object-store base-backup tar parts fully drained. Against bootstrap_parts_total this is the remaining-work denominator, and it keeps advancing through TOAST-heavy parts unlike bootstrap_bytes_tapped.",
    counter bootstrap_bytes_tapped: u64 =
        "Backup body bytes the bootstrap pump handed to the page-walk sink.",
    counter bootstrap_pages_walked: u64 = "8 KiB heap pages the bootstrap walk framed.",
    counter bootstrap_tuples_emitted: u64 =
        "Live tuples the bootstrap walk decoded off backup pages.",
    counter bootstrap_files_walked: u64 = "User-heap segments the bootstrap walk decoded.",
    counter bootstrap_files_skipped_unmapped: u64 =
        "User-heap segments declined at begin because no mapped relation owns them; their bytes drain unread.",
    counter bootstrap_decode_seconds: f64 =
        "Cumulative CPU inside the bootstrap page walk, tuple decode included.",
    counter bootstrap_tap_seconds: f64 =
        "Cumulative time bootstrap tap readers spent inside the sink: page framing, decode, channel send. Against bootstrap_decode_seconds this is the tap's own overhead.",
    counter bootstrap_channel_block_seconds: f64 =
        "Cumulative time the bootstrap walk spent waiting for a free tuple-channel slot, i.e. emitter drain time seen by the walk.",
    counter spill_evictions_total: u64 = "Total evictions in→spill since daemon start.",
    counter xacts_committed_total: u64 = "Total xacts drained as commits since daemon start.",
    counter xacts_aborted_total: u64 = "Total xacts dropped as aborts since daemon start.",
    counter decoder_decoded_total: u64 = "Heap records decoded since daemon start.",
    counter decoder_records_by_op: [u64; 5]
        ["op" = ["insert", "update", "hot_update", "delete", "truncate"]] =
        "Decoded heap row changes by operation.",
    counter decoder_buffer_lock_acquires_total: u64 =
        "Acquisitions of the shared xact-buffer mutex from the per-record decode path, contended or not.",
    counter decoder_buffer_lock_waits_total: u64 =
        "Acquisitions of the shared xact-buffer mutex that found it held and had to block (a subset of acquires).",
    counter decoder_buffer_lock_wait_seconds_total: f64 =
        "Cumulative time the per-record decode path blocked on the xact-buffer mutex. rate() is workers' worth of time lost to it; divided by the waits counter it is per-wait latency.",
    counter decoder_partial_total: u64 = "Decoded tuples with prefix/suffix-from-old elided columns.",
    counter decoder_toast_chunks_total: u64 =
        "TOAST chunks routed into the xact buffer's chunk slot.",
    counter decoder_toast_malformed_total: u64 =
        "TOAST inserts the decoder couldn't reinterpret as a chunk.",
    counter decoder_toast_deletes_total: u64 =
        "DELETE records on toast relations, buffered as tombstone rows.",
    counter decoder_catalog_not_found_total: u64 =
        "Heap records skipped because no descriptor covered their relfilenode.",
    counter decoder_skipped_op_total: u64 =
        "Heap records that decoded to no row change. LOCK, CONFIRM and heap2 page maintenance drop earlier, uncounted.",
    counter decoder_skipped_no_block_total: u64 =
        "Heap records skipped for carrying no block reference.",
    counter toast_chunks_stored_total: u64 =
        "TOAST chunk rows persisted to the CH store; the bootstrap's TOAST-phase progress signal.",
    counter toast_chunk_puts_total: u64 =
        "Chunk-store INSERTs issued. Against the seconds counter this gives per-part commit latency, which caps a TOAST-heavy restore.",
    counter toast_chunk_put_seconds: f64 =
        "Cumulative wall-clock inside chunk-store INSERTs. Divided by toast_chunk_puts_total this is per-part commit latency.",
    counter toast_tombstones_stored_total: u64 =
        "TOAST delete tombstone rows persisted to the CH store.",
    counter toast_values_fetched_total: u64 =
        "Values reassembled out of the CH store rather than the in-xact buffer: pre-window re-emits and the bootstrap's deferred referrers.",
    counter toast_value_fetch_batches_total: u64 =
        "Store fetch round trips. Against toast_values_fetched_total this is values per query, the deferred-resolution batching factor.",
    counter toast_value_fetch_seconds: f64 =
        "Cumulative wall-clock inside chunk-store fetches. Divided by toast_value_fetch_batches_total this is per-query latency.",
    counter toast_values_read_ahead_total: u64 =
        "Values read from the store ahead of planning, off the serial path. The fetch counters include these reads.",
    counter toast_value_wait_seconds: f64 =
        "Cumulative wall-clock planning spent waiting on values read ahead of it, apart from the store time those reads count in toast_value_fetch_seconds.",
    counter toast_image_rows_mirrored_total: u64 =
        "Chunk rows mirrored from restored page images during a backup window; non-zero means the backup copied TOAST pages mid-write.",
    counter toast_values_filled_superseded_total: u64 =
        "Store-mode values filled after their history merge-collapsed.",
    counter toast_values_filled_mismatch_total: u64 =
        "Store-mode values filled off a dense-but-short store run (partial collapse or generation mixing).",
    counter toast_values_filled_generation_total: u64 =
        "Shadow values replaced with a fill after detecting value-ID reuse.",
    counter toast_values_filled_oversize_total: u64 =
        "Values over inline_value_max replaced with NULL or a column default.",
    counter toast_fetch_miss_total: u64 =
        "TOAST resolution failures caused by missing chunks or size mismatches.",
    counter bootstrap_replay_covered_rows_total: u64 =
        "Backup rows dropped during deferred TOAST resolution because values were unreadable and window replay emitted a tombstone or newer version for each tuple's slot.",
    counter bootstrap_gate_emitted_total: u64 =
        "Heap tuples forwarded by bootstrap visibility gate using hint bits.",
    counter bootstrap_gate_gated_total: u64 =
        "Heap tuples dropped by bootstrap visibility gate using hint bits.",
    counter bootstrap_gate_deferred_total: u64 =
        "Heap tuples with undecided hint-bit visibility, spooled for transaction-status checks.",
    counter bootstrap_gate_chunks_gated_total: u64 =
        "TOAST chunk tuples excluded from mirror writes because hint bits proved them dead.",
    counter bootstrap_gate_multixact_emitted_total: u64 =
        "Heap tuples forwarded by hint-bit visibility checks with multixact xmax.",
    counter toast_mirror_truncates_total: u64 =
        "Mirror wipes from owner TRUNCATE, applied at the reorder barrier.",
    counter toast_mirror_retires_total: u64 =
        "Mirrors emptied because their toast rel dropped (owner DROP / rewrite); table retained.",
    counter toast_rewrite_barriers_total: u64 =
        "Rewrite generations closed with residual O-B tombstones.",
    counter toast_stash_buffered_total: u64 =
        "Records on marker-proven invisible filenodes stashed raw for commit-time resolution.",
    counter raw_stash_deferred_total: u64 =
        "Records held raw because their xact tree wrote catalog state earlier in the stream; resolved at commit.",
    counter toast_stash_decoded_total: u64 =
        "Stashed records decoded at commit against a resolved toast heap.",
    counter toast_stash_discarded_total: u64 =
        "Stashed records discarded: filenode unresolvable post-commit (dropped or rotated away).",
    counter toast_stash_in_place_total: u64 =
        "Stashed toast filenodes that superseded no predecessor, so queued no residual barrier.",
    counter stash_foreign_db_skipped_total: u64 =
        "Stashed filenodes resolved to a foreign database at commit, counted once per filenode.",
    counter xact_plan_rows: u64 = "Routed heaps sealed into transaction plans.",
    /// Sealed plan bytes `[mem, file]`, rendered `storage=` labelled
    counter xact_plan_bytes_by_storage: [u64; 2] ["storage" = ["mem", "file"]] =
        "Sealed transaction-plan bytes by final backing.",
    /// Planning failures `[spool, fail_closed, detoast, partial_update,
    /// view, drain]`, rendered `reason=` labelled
    counter xact_plan_failures_by_reason: [u64; 11] ["reason" = PLAN_FAILURE_REASONS] =
        "Planning-stage failures; the whole transaction emits nothing.",
    counter raw_stash_bytes_by_storage: [u64; 2] ["storage" = ["mem", "spill"]] =
        "Cumulative raw-stash payload bytes by first landing.",
    counter raw_decode_rows_by_op: [u64; 7] ["op" = HEAP_OP_LABELS] =
        "Rows fanned out of decoded raw records.",
    gauge raw_pending_rows: u64 = "Raw-decoded heaps queued for pending-first yield.",
    gauge raw_pending_bytes: u64 = "Bytes held by the pending raw fanout.",
    counter backfill_backup_rows_total: u64 = "Tuples decoded by table backfill backup walks, including TOAST chunks and rows awaiting visibility checks. Cumulative across passes.",
    counter backfill_backup_bytes_total: u64 = "Backup body bytes handed to table backfill page walks, excluding skipped files. Cumulative across passes.",
    counter backfill_copy_rows_total: u64 = "Rows decoded from source COPY for table backfills.",
    counter backfill_copy_bytes_total: u64 = "Field payload bytes decoded from source COPY, excluding binary framing.",
    counter emitter_rows_total: u64 = "Rows the CH emitter has handed to send_data.",
    counter emitter_blocks_total: u64 = "Native blocks the CH emitter has written.",
    counter emitter_xacts_total: u64 = "Xacts the CH emitter has drained.",
    counter emitter_unsupported_relations: u64 =
        "Tuples skipped because the source relation has no mapping in --ch-config.",
    counter emitter_deletes_discarded: u64 =
        "DELETE rows dropped because is_deleted = false leaves no marker column.",
    counter emitter_retries_attempted_total: u64 =
        "Failed ClickHouse operations retried, one per failing operation rather than per attempt.",
    counter emitter_reconnects_total: u64 =
        "ClickHouse client dials after the first, from retries and live endpoint or compression changes.",
    gauge pump_queue_depth: u64 =
        "Records buffered between WAL pump and queueing worker, including records processed only for their positions.",
    counter queue_records_out_total: u64 =
        "Records the queueing/reorder worker has dequeued and dispatched. rate() is the worker's throughput; with pump_queue_depth it tells deep-and-draining from deep-and-stalled.",
    counter queue_jobs_out_total: u64 =
        "DecodeJobs the queueing worker shipped to the decode pool. queue_jobs_out - decode_jobs_in is the worker->pool channel depth.",
    counter decode_jobs_in_total: u64 =
        "DecodeJobs the decode pool has pulled. Pinned at queue_jobs_out ⇒ pool idle; the gap at the channel cap ⇒ the pool is the limiter.",
    counter decode_rows_out_total: u64 =
        "Rows the decode pool routed to the insertbatch builder. decode_rows_out - insertbatch_rows_in is the pool->builder channel depth.",
    counter insertbatch_rows_in_total: u64 =
        "Rows the insertbatch builder accepted before sealing into InsertBatches.",
    counter insertbatch_batches_out_total: u64 =
        "InsertBatches the builder sealed and pushed to the inserter pool.",
    counter inserter_batches_in_total: u64 =
        "InsertBatches an inserter finished draining to ClickHouse. insertbatch_batches_out - inserter_batches_in is the live backlog; their rates show inserter-pool saturation.",
    counter inserter_ch_seconds_total: f64 =
        "Cumulative inserter time inside the ClickHouse INSERT round trip. Against inserter_pool_size x uptime this is CH-side utilization.",
    counter inserter_encode_seconds_total: f64 =
        "Cumulative inserter time rebuilding the Native block over a batch's owned slabs.",
    counter oracle_resolve_seconds_total: f64 =
        "Cumulative resolver time inside the oracle round trip. Overlaps inserter_ch_seconds_total, so the larger of the two is the tail's limiter.",
    counter process_cpu_seconds_total: f64 =
        "Total user+system CPU seconds consumed by the walshadow process.",
    gauge process_resident_memory_bytes: u64 = "Resident set size of the walshadow process (VmRSS).",
    gauge process_threads: u64 = "OS threads in walshadow, including async and blocking workers.",
    counter oracle_local_columns_total: u64 =
        "Oracle-routed columns the daemon built itself: already-rendered cells against a String target, which PG would hand straight back.",
    counter oracle_blocks_total: u64 =
        "Partial Native blocks the walshadow extension returned and the daemon validated.",
    counter oracle_rows_total: u64 = "Rows carried by those blocks.",
    counter oracle_cells_total: u64 = "Source cells sent for conversion, rows times oracle columns.",
    counter oracle_conversion_errors_total: u64 =
        "Requests the extension failed on one PG Datum it could not convert.",
    counter oracle_errors_total: u64 = "Oracle request, transport, or response-validation failures.",
    counter uptime_seconds: u64 = "Seconds since the daemon began its status loop.",
    gauge bootstrap_attempt: u32 =
        "Which attempt the running initial load is; above 1 means an incomplete one was discarded and re-extracted.",
    counter archive_wal_segments_total: u64 =
        "WAL segments replayed out of the backup archive because the source could not serve the resume point.",
    counter archive_fetch_seconds_total: f64 =
        "Summed archive fetch durations, including download, decompression and local staging; overlapping fetches add.",
    counter archive_wait_seconds_total: f64 =
        "Pump time awaiting next prefetched WAL segment, including waits spanning status ticks.",
    counter pump_queue_wait_seconds_total: f64 =
        "Pump time sending batches into bounded decoder queue.",
    counter archive_replay_seconds_total: f64 =
        "Time filtering and dispatching archived WAL, including downstream backpressure.",
    gauge archive_restore_active: u64 =
        "1 while consuming prefetched archive WAL through the normal pump.",
    counter source_endpoint_swaps_total: u64 =
        "Source feeds swapped onto a reloaded `[source]` endpoint or slot.",
    counter source_endpoint_swap_failures_total: u64 =
        "Swap attempts refused or unreachable (identity proof, connect, START_REPLICATION, missing slot).",
    gauge source_endpoint_swap_pending: u64 =
        "1 while config names a source endpoint or slot the pump has not reached yet.",
    gauge source_timeline: u32 = "Source WAL timeline the pump is reading.",
    gauge floor_timeline: u32 = "Timeline owning the durable floor, which restart resumes on.",
    counter timeline_switches_total: u64 = "Source timeline crossings completed.",
    /// Crossings refused, rendered `reason=` labelled; order matches
    /// [`SWITCH_FAILURE_REASONS`]
    counter timeline_switch_failures_by_reason: [u64; SWITCH_FAILURE_REASONS.len()]
        ["reason" = SWITCH_FAILURE_REASONS] =
        "Crossings refused; the stream stays on the ancestor.",
    /// Most recent fork point, diagnostic
    gauge timeline_switch_lsn: u64 = "Fork LSN of the most recent timeline crossing.",
    counter timeline_prefix_bytes_verified_total: u64 =
        "Descendant bytes compared against the retained ancestor fork prefix.",
    counter timeline_transition_seconds_total: f64 = "Seconds spent inside timeline crossings.",
    /// `WalStream::next_lsn` frozen when the pump observed `[stream] paused`,
    /// which resume asks the source to serve. `0` while not paused: a value
    /// left from an earlier pause cannot be compared against a promotion
    /// decision either
    gauge pause_consumed_lsn: u64 =
        "Consumed frontier frozen when the pump observed the pause; 0 while running.",
    /// Source head last heard about, frozen at the same instant. The promotion
    /// target must reach it before it is promoted
    gauge pause_received_lsn: u64 =
        "Source head frozen when the pump observed the pause; 0 while running.",
    gauge shadow_served_timeline: u32 = "Timeline the shadow-facing walsender advertises.",
    /// Branch the shadow is replaying, which is how a shadow that followed the
    /// chain reads differently from one that merely survived
    gauge shadow_replay_timeline: u32 = "Timeline the shadow is replaying.",
    /// `source_received_lsn - min_apply_lsn` across active shadow walreceivers.
    /// Holds its last value while no apply LSN is known (before the first
    /// walreceiver status after a daemon start).
    /// Caller saturates to 0 when shadow is ahead; passes `source_received_lsn`
    /// when none connected (disconnect = max lag)
    gauge shadow_apply_lag_bytes: u64 =
        "Bytes between source_received_lsn and the min apply LSN reported by active shadow walreceivers.",
    /// `shadow_apply_lag_bytes` / rolling 30s WAL byte-rate estimate.
    /// `f64::INFINITY` (renders `+Inf`) when rate is 0
    gauge shadow_apply_lag_seconds: f64 =
        "Estimated seconds shadow trails source, by source byte rate over last 30s.",
    gauge shadow_stream_active_connections: u64 =
        "Currently-attached walreceiver connections to walshadow's walsender.",
    /// Cumulative connections dropped by `slow_threshold` overflow
    counter shadow_stream_dropped_connections_total: u64 =
        "Connections dropped by slow-client cutoff since daemon start.",
    counter catalog_boundary_holds_total: u64 =
        "Publication holds released at catalog-mutating commit boundaries.",
    counter catalog_boundary_hold_failures_total: u64 =
        "Catalog-boundary holds woken with an error (worker death, walreceiver loss, timeout).",
    counter catalog_boundary_hold_seconds_total: f64 =
        "Cumulative seconds the pump parked in released catalog-boundary holds.",
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SkippedRel {
    pub database: String,
    pub rel_node: String,
    pub op: &'static str,
    pub reason: &'static str,
    pub records: u64,
}

/// Declares [`DbSeries`] and the families a scrape renders off it, one
/// `database=` labelled sample per followed database. Same field syntax as
/// [`snapshot!`]; a `["key" = LABELS]` array carries its own label beside the
/// database's
macro_rules! db_snapshot {
    (
        custom { $($(#[doc = $custom_doc:literal])* $custom:ident: $custom_ty:ty,)* }
        $(
            $(#[doc = $doc:literal])*
            $kind:ident $field:ident: $ty:ty $([$key:literal = $labels:expr])? = $help:literal,
        )*
    ) => {
        /// One source database's share of the work: its bridge sockets,
        /// descriptor log, catalog capture and config overlay. Values are
        /// that database's own, never a cluster total
        #[derive(Debug, Default, Clone)]
        pub struct DbSeries {
            /// `database=` label value, from `[source] databases`
            pub database: String,
            $($(#[doc = $custom_doc])* pub $custom: $custom_ty,)*
            $(
                #[doc = $help]
                $(#[doc = $doc])*
                pub $field: $ty,
            )*
        }

        fn encode_db_fields(dbs: &[DbSeries], enc: &mut DescriptorEncoder<'_>) -> fmt::Result {
            let names: Vec<Cow<'_, str>> =
                dbs.iter().map(|db| escape_label(&db.database)).collect();
            $(encode_db_metric!(
                enc,
                dbs,
                names,
                $kind,
                concat!("walshadow_", stringify!($field)),
                $help,
                $field
                $(, $key, $labels)?
            );)*
            Ok(())
        }
    };
}

db_snapshot! {
    custom {
        /// This database's `initial_load` backfills recorded in the ledger
        /// but not yet complete. Only `ctl status` reads it; the scrape
        /// renders the `mode=` split beside it
        config_backfills_pending: u64,
    }

    gauge bridge_up: u64 =
        "1 while the pgext bridge worker answered the last request over its socket.",
    /// Per-op, rendered `op=` labelled; order matches
    /// [`OP_LABELS`].
    counter bridge_requests_by_op: [u64; OP_COUNT] ["op" = OP_LABELS] =
        "Requests sent to the pgext bridge worker.",
    counter bridge_errors_by_op: [u64; OP_COUNT] ["op" = OP_LABELS] =
        "Bridge requests that failed, transport or worker-side.",
    counter bridge_request_seconds_by_op: [f64; OP_COUNT] ["op" = OP_LABELS] =
        "Wall time spent in bridge round trips.",
    /// Queued behind another caller on the single bridge socket
    counter bridge_lock_wait_seconds_by_op: [f64; OP_COUNT] ["op" = OP_LABELS] =
        "Wall time bridge callers spent queued for the socket. Against bridge_service_seconds this says whether the worker or the funnel in front of it is the limiter.",
    /// Wire time with the socket held
    counter bridge_service_seconds_by_op: [f64; OP_COUNT] ["op" = OP_LABELS] =
        "Wall time on the wire with the bridge socket held: worker conversion plus transfer.",
    counter bridge_request_bytes_by_op: [u64; OP_COUNT] ["op" = OP_LABELS] =
        "Request frame bytes written to the bridge socket.",
    counter bridge_response_bytes_by_op: [u64; OP_COUNT] ["op" = OP_LABELS] =
        "Response frame bytes read back off the bridge socket.",
    counter bridge_reconnects_total: u64 =
        "Bridge sockets redialled after a worker exit or transport error.",
    counter bridge_scan_rows_total: u64 = "Catalog rows the bridge's overlay scans returned.",
    counter bridge_scan_subtrans_mismatch_total: u64 =
        "Overlay tuples whose writer did not resolve to the requested top xid; trusted as ours only on rel-scoped catalogs.",
    counter bridge_scan_replay_moved_total: u64 =
        "Bridge scans that found shadow replay off the position their read pinned; committed reads answer these off SQL instead.",
    counter bridge_native_bytes_total: u64 =
        "Native block bytes the bridge returned for ENCODE_NATIVE requests.",
    counter desc_capture_sql_total: u64 = "Catalog boundaries captured via shadow SQL fan-out.",
    counter desc_capture_log_replay_total: u64 =
        "Catalog boundaries replayed from stored descriptor-log batches.",
    counter desc_capture_skipped_covered_total: u64 =
        "Boundaries at or below the seed's covered_through, skipped.",
    counter desc_capture_all_total: u64 =
        "Capture-all boundaries (whole-relcache inval / pg_namespace write).",
    counter desc_capture_rels_total: u64 = "Descriptors fetched across SQL captures.",
    counter desc_capture_seconds_total: f64 =
        "Cumulative seconds inside descriptor capture (within the boundary hold).",
    counter desc_events_added_total: u64 = "Added schema events produced by descriptor capture.",
    counter desc_events_changed_total: u64 = "Changed schema events produced by descriptor capture.",
    counter desc_events_dropped_total: u64 = "Dropped schema events produced by descriptor capture.",
    counter pending_captures_total: u64 =
        "Command boundaries read into the pending catalog timeline.",
    counter pending_rels_total: u64 = "Descriptors read across command boundaries.",
    counter pending_holds_total: u64 = "Publication holds taken for a command boundary.",
    counter pending_hold_seconds_total: f64 =
        "Cumulative seconds the pump parked in command-boundary holds.",
    counter pending_entries_promoted_total: u64 =
        "Pending slots folded into a commit's descriptor-log batch.",
    counter pending_entries_dropped_abort_total: u64 =
        "Pending slots dropped with an aborted transaction tree.",
    counter pending_ambiguities_suppressed_total: u64 =
        "Ambiguity intervals the pending timeline covered end to end.",
    counter pending_degraded_by_reason: [u64; 5]
        ["reason" = DegradeReason::ALL.map(|reason| reason.label())] =
        "Transactions degraded to commit-time capture, by reason.",
    gauge desc_log_entries: u64 = "Descriptor-log index entries resident.",
    gauge desc_log_tail_bytes: u64 = "Descriptor-log tail bytes since last checkpoint.",
    gauge desc_log_batches: u64 = "Descriptor-log batches resident.",
    counter desc_log_gc_total: u64 = "Descriptor-log checkpoint compactions.",
    counter desc_log_gc_dropped_entries_total: u64 = "Entries dropped by descriptor-log GC.",
    counter desc_lookups_present_total: u64 = "Descriptor lookups answered Present.",
    counter desc_lookups_dropped_total: u64 = "Descriptor lookups answered Dropped.",
    counter desc_lookups_retired_total: u64 =
        "Descriptor lookups answered Retired (rotated-away filenode).",
    counter desc_lookups_ambiguous_total: u64 =
        "Descriptor lookups landing in an ambiguity interval.",
    counter descriptor_ambiguous_total: u64 =
        "Ambiguity intervals published at capture for unproven in-place changes.",
    counter desc_lookups_not_covered_total: u64 = "Descriptor lookups answered NotCovered.",
    counter desc_lookups_foreign_db_total: u64 =
        "Descriptor lookups on a foreign database's filenode.",
    gauge config_pending_decl_rels: u64 =
        "Forward-declared per-table opt-ins awaiting their CREATE TABLE.",
    /// Cumulative `replicate=true` materialisations / `replicate=false`
    /// exclusions applied via the config overlay.
    counter config_replicate_opt_in_total: u64 =
        "Total config_table.replicate=true materialisations applied.",
    counter config_replicate_opt_out_total: u64 =
        "Total config_table.replicate=false / removals applied.",
    gauge config_backfills_pending_by_mode: [u64; 3] ["mode" = BACKFILL_MODES] =
        "initial_load backfills recorded in the ledger but not yet complete.",
}

#[derive(Debug, Clone, Default)]
pub struct MetricsRegistry {
    inner: Arc<RwLock<MetricsSnapshot>>,
    health: Arc<Health>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Readiness inputs `/readyz` answers from
    pub fn health(&self) -> &Arc<Health> {
        &self.health
    }

    /// Single writer (status-line loop), so the write lock is uncontended.
    pub async fn set(&self, snap: MetricsSnapshot) {
        *self.inner.write().await = snap;
    }

    pub async fn update(&self, edit: impl FnOnce(&mut MetricsSnapshot)) {
        edit(&mut *self.inner.write().await);
    }

    pub async fn snapshot(&self) -> MetricsSnapshot {
        self.inner.read().await.clone()
    }
}

/// What `/readyz` answers from. Beside the snapshot rather than in it: the
/// publishers replace the snapshot wholesale, and these flags have one owner
/// each, the session and the shadow supervisor
#[derive(Debug, Default)]
pub struct Health {
    /// Bootstrap is done and the session's pump loop runs
    streaming: AtomicBool,
    /// The daemon-owned shadow postmaster answered its supervisor's last probe
    shadow_up: AtomicBool,
}

impl Health {
    /// Reads as streaming until the guard drops, which every exit from the
    /// pump loop does, error and panic included
    pub fn streaming(self: &Arc<Self>) -> StreamingGuard {
        self.streaming.store(true, Ordering::Relaxed);
        StreamingGuard(self.clone())
    }

    pub fn set_shadow_up(&self, up: bool) {
        self.shadow_up.store(up, Ordering::Relaxed);
    }

    /// `Err` names the first condition keeping the daemon from ready. Only the
    /// daemon's own state counts: a paused pump, an unreachable source or a
    /// destination outage leaves it ready, since a restart fixes none of them
    pub fn readiness(&self) -> Result<(), &'static str> {
        if !self.streaming.load(Ordering::Relaxed) {
            return Err("not streaming yet: bootstrap or startup still running");
        }
        if !self.shadow_up.load(Ordering::Relaxed) {
            return Err("shadow postmaster is down");
        }
        Ok(())
    }
}

/// Clears [`Health`]'s streaming flag on drop
#[must_use = "the session reads as streaming only while the guard lives"]
#[derive(Debug)]
pub struct StreamingGuard(Arc<Health>);

impl Drop for StreamingGuard {
    fn drop(&mut self) {
        self.0.streaming.store(false, Ordering::Relaxed);
    }
}

/// Rolling 30s ring of `(timestamp, source_received_lsn)` samples, deriving a
/// coarse WAL byte-rate for `shadow_apply_lag_seconds`.
#[derive(Debug)]
pub struct RateEstimator {
    window: Duration,
    samples: VecDeque<(Instant, u64)>,
}

impl RateEstimator {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            samples: VecDeque::new(),
        }
    }

    /// Push a sample, prune entries older than `window`.
    pub fn observe(&mut self, now: Instant, received_lsn: u64) {
        self.samples.push_back((now, received_lsn));
        let cutoff = now.checked_sub(self.window);
        while let Some(&(t, _)) = self.samples.front()
            && cutoff.is_some_and(|c| t < c)
            && self.samples.len() > 1
        {
            self.samples.pop_front();
        }
    }

    /// Bytes-per-second across the ring. `None` if < 2 samples or zero elapsed.
    pub fn rate(&self) -> Option<f64> {
        let (front_t, front_lsn) = *self.samples.front()?;
        let (back_t, back_lsn) = *self.samples.back()?;
        let elapsed = back_t.saturating_duration_since(front_t).as_secs_f64();
        if elapsed <= 0.0 {
            return None;
        }
        let delta = back_lsn.saturating_sub(front_lsn);
        if delta == 0 {
            return None;
        }
        Some(delta as f64 / elapsed)
    }

    /// Lag bytes → seconds against current rate. `0.0` for zero lag,
    /// `INFINITY` for unknown rate.
    pub fn seconds_for(&self, lag_bytes: u64) -> f64 {
        if lag_bytes == 0 {
            return 0.0;
        }
        match self.rate() {
            Some(r) if r > 0.0 => lag_bytes as f64 / r,
            _ => f64::INFINITY,
        }
    }
}

impl Default for RateEstimator {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

/// `reason=` label order of `MetricsSnapshot::xact_plan_failures_by_reason`,
/// matching [`crate::emit::pipeline::planner::drain_reason`]
const PLAN_FAILURE_REASONS: [&str; 11] = [
    "spool",
    "fail_closed_image_only",
    "fail_closed_malformed",
    "fail_closed_unsupported_op",
    "stash_ambiguous",
    "incomplete_toast",
    "missing_stash_resolution",
    "detoast",
    "partial_update",
    "view",
    "drain",
];

/// `mode=` label order of [`DbSeries::config_backfills_pending_by_mode`]
const BACKFILL_MODES: [&str; 3] = ["copy", "base_backup", "object_store"];

/// Family declaration off a snapshot field name: a `_by_<label>` tail names
/// the label rather than the family, and OpenMetrics appends `_total` to
/// counter samples itself, so a counter family declares the name without it
fn declare<'s>(
    enc: &'s mut DescriptorEncoder<'_>,
    name: &'s str,
    help: &str,
    kind: MetricType,
) -> Result<MetricEncoder<'s>, fmt::Error> {
    let name = name.rsplit_once("_by_").map_or(name, |(head, _)| head);
    let name = if matches!(kind, MetricType::Counter) {
        name.strip_suffix("_total").unwrap_or(name)
    } else {
        name
    };
    enc.encode_descriptor(name, help, None, kind)
}

pub(crate) fn counter<V: EncodeCounterValue>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    value: V,
) -> fmt::Result {
    declare(enc, name, help, MetricType::Counter)?
        .encode_counter::<NoLabelSet, _, u64>(&value, None)
}

pub(crate) fn gauge<V: EncodeGaugeValue>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    value: V,
) -> fmt::Result {
    declare(enc, name, help, MetricType::Gauge)?.encode_gauge(&value)
}

/// One `key=` labelled series per `(label, value)`, under one declaration
pub(crate) fn counter_series<'a, V: EncodeCounterValue>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    key: &str,
    series: impl IntoIterator<Item = (&'a str, V)>,
) -> fmt::Result {
    let mut family = declare(enc, name, help, MetricType::Counter)?;
    for (label, value) in series {
        family
            .encode_family(&[(key, label)])?
            .encode_counter::<NoLabelSet, _, u64>(&value, None)?;
    }
    Ok(())
}

pub(crate) fn counter_pairs<'a, V: EncodeCounterValue, const N: usize>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    keys: [&str; N],
    series: impl IntoIterator<Item = ([&'a str; N], V)>,
) -> fmt::Result {
    let mut family = declare(enc, name, help, MetricType::Counter)?;
    for (values, value) in series {
        let labels: [(&str, &str); N] = std::array::from_fn(|i| (keys[i], values[i]));
        family
            .encode_family(&labels)?
            .encode_counter::<NoLabelSet, _, u64>(&value, None)?;
    }
    Ok(())
}

pub(crate) fn gauge_series<'a, V: EncodeGaugeValue>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    key: &str,
    series: impl IntoIterator<Item = (&'a str, V)>,
) -> fmt::Result {
    let mut family = declare(enc, name, help, MetricType::Gauge)?;
    for (label, value) in series {
        family
            .encode_family(&[(key, label)])?
            .encode_gauge(&value)?;
    }
    Ok(())
}

/// `kind=`/`op=` grid over [`HEAP_OP_LABELS`]
fn counter_kind_op(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    kinds: [&str; 2],
    grid: &[[u64; 7]; 2],
) -> fmt::Result {
    let mut family = declare(enc, name, help, MetricType::Counter)?;
    for (kind, ops) in kinds.into_iter().zip(grid) {
        for (op, value) in HEAP_OP_LABELS.into_iter().zip(ops) {
            family
                .encode_family(&[("kind", kind), ("op", op)])?
                .encode_counter::<NoLabelSet, _, u64>(value, None)?;
        }
    }
    Ok(())
}

/// `database=`/`key=` grid, one row of `(label, value)` pairs per database
fn counter_grid<'a, V, I>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    key: &str,
    rows: impl IntoIterator<Item = (&'a str, I)>,
) -> fmt::Result
where
    V: EncodeCounterValue,
    I: IntoIterator<Item = (&'a str, V)>,
{
    let mut family = declare(enc, name, help, MetricType::Counter)?;
    for (database, series) in rows {
        for (label, value) in series {
            family
                .encode_family(&[("database", database), (key, label)])?
                .encode_counter::<NoLabelSet, _, u64>(&value, None)?;
        }
    }
    Ok(())
}

fn gauge_grid<'a, V, I>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    key: &str,
    rows: impl IntoIterator<Item = (&'a str, I)>,
) -> fmt::Result
where
    V: EncodeGaugeValue,
    I: IntoIterator<Item = (&'a str, V)>,
{
    let mut family = declare(enc, name, help, MetricType::Gauge)?;
    for (database, series) in rows {
        for (label, value) in series {
            family
                .encode_family(&[("database", database), (key, label)])?
                .encode_gauge(&value)?;
        }
    }
    Ok(())
}

/// The encoder writes label values through, and a database name is whatever
/// `CREATE DATABASE` took, so escape what OpenMetrics reserves inside quotes
fn escape_label(value: &str) -> Cow<'_, str> {
    if !value.contains(['\\', '"', '\n']) {
        return Cow::Borrowed(value);
    }
    let mut out = String::with_capacity(value.len() + 8);
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    Cow::Owned(out)
}

/// LSN gauges carry their raw `u64`
impl<K> EncodeGaugeValue for Pos<K> {
    fn encode(&self, encoder: &mut GaugeValueEncoder) -> fmt::Result {
        EncodeGaugeValue::encode(&self.get(), encoder)
    }
}

/// One tenant's view: where it stands and how far it trails the pump
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TenantMetrics {
    pub id: String,
    pub dbname: String,
    /// `priming`, `active` or `detached`
    pub phase: &'static str,
    pub ack_lsn: u64,
    pub resume_safe_lsn: u64,
    /// Bytes between the pump's routing position and the tenant's resume floor
    pub lag_bytes: u64,
    pub queue_depth: u64,
    pub xacts_active: u64,
    pub rows_emitted: u64,
    pub backfill_copy_rows: u64,
}

fn encode_tenants(tenants: &[TenantMetrics], enc: &mut DescriptorEncoder<'_>) -> fmt::Result {
    if tenants.is_empty() {
        return Ok(());
    }
    let mut info = declare(
        enc,
        "walshadow_tenant_info",
        "Configured tenant: followed database and lifecycle phase.",
        MetricType::Gauge,
    )?;
    for t in tenants {
        info.encode_family(&[
            ("tenant", t.id.as_str()),
            ("dbname", t.dbname.as_str()),
            ("phase", t.phase),
        ])?
        .encode_gauge(&1u64)?;
    }
    type Pick = fn(&TenantMetrics) -> u64;
    let gauges: [(&str, &str, Pick); 6] = [
        (
            "walshadow_tenant_ack_lsn",
            "Tenant's contiguous destination acknowledgment.",
            |t| t.ack_lsn,
        ),
        (
            "walshadow_tenant_resume_safe_lsn",
            "Lowest position a restart replays for this tenant; the slot holds WAL back to the least of these.",
            |t| t.resume_safe_lsn,
        ),
        (
            "walshadow_tenant_lag_bytes",
            "Bytes between the pump's routing position and the tenant's resume floor.",
            |t| t.lag_bytes,
        ),
        (
            "walshadow_tenant_queue_depth",
            "Records queued for the tenant's decoder.",
            |t| t.queue_depth,
        ),
        (
            "walshadow_tenant_xacts_active",
            "Transactions the tenant is buffering.",
            |t| t.xacts_active,
        ),
        (
            "walshadow_tenant_backfill_copy_rows",
            "Rows the tenant's COPY initial loads shipped.",
            |t| t.backfill_copy_rows,
        ),
    ];
    for (name, help, pick) in gauges {
        let mut family = declare(enc, name, help, MetricType::Gauge)?;
        for t in tenants {
            family
                .encode_family(&[("tenant", t.id.as_str())])?
                .encode_gauge(&pick(t))?;
        }
    }
    let mut rows = declare(
        enc,
        "walshadow_tenant_rows_emitted_total",
        "Rows the tenant's pipeline delivered to its destination.",
        MetricType::Counter,
    )?;
    for t in tenants {
        rows.encode_family(&[("tenant", t.id.as_str())])?
            .encode_counter::<NoLabelSet, _, u64>(&t.rows_emitted, None)?;
    }
    Ok(())
}

/// Snapshot owned for the length of a scrape, so encoding runs without the
/// registry lock
#[derive(Debug)]
struct SnapshotCollector(MetricsSnapshot);

impl Collector for SnapshotCollector {
    fn encode(&self, mut enc: DescriptorEncoder) -> fmt::Result {
        encode_snapshot(&self.0, &mut enc)
    }
}

/// `snapshot!`-declared families, then the `custom` ones
fn encode_snapshot(snap: &MetricsSnapshot, enc: &mut DescriptorEncoder<'_>) -> fmt::Result {
    encode_fields(snap, enc)?;
    encode_tenants(&snap.tenants, enc)?;

    counter_pairs(
        enc,
        "walshadow_cells_inserted",
        "Cells inserted per ClickHouse column type, split by whether the shadow oracle encoded them.",
        ["type", "encoding"],
        snap.cells_inserted_by_type
            .iter()
            .map(|(ty, encoding, n)| ([ty.as_str(), *encoding], *n)),
    )?;

    counter_pairs(
        enc,
        "walshadow_table_rows_inserted",
        "Rows inserted per source table, initial load and CDC alike.",
        ["database", "table"],
        snap.rows_inserted_by_table
            .iter()
            .map(|(database, table, n)| ([database.as_str(), table.as_str()], *n)),
    )?;

    counter_pairs(
        enc,
        "walshadow_decoder_skipped_records",
        "Records skipped by the decoder or dropped by the filter, by relfilenode, op and reason. Heaviest series only.",
        ["database", "rel_node", "op", "reason"],
        snap.skipped_records_by_rel.iter().map(|r| {
            (
                [r.database.as_str(), r.rel_node.as_str(), r.op, r.reason],
                r.records,
            )
        }),
    )?;

    gauge(
        enc,
        "walshadow_crossing_wedged",
        "1 while a crossing is parked on a refusal only an operator can clear.",
        u64::from(!snap.crossing_blocked_on.is_empty()),
    )?;

    // rmgr names come out of the fixed resource-manager table, so label values
    // need no escaping the encoder does not do
    let mut records = declare(
        enc,
        "walshadow_filter_records_total",
        "Records observed by the filter, labeled by rmgr + route.",
        MetricType::Counter,
    )?;
    for ((rm, route), n) in &snap.records_by_rm_route {
        records
            .encode_family(&[("rmgr", rm.as_str()), ("route", *route)])?
            .encode_counter::<NoLabelSet, _, u64>(n, None)?;
    }

    counter_kind_op(
        enc,
        "walshadow_raw_stash_records_total",
        "Records stashed raw for commit-time resolution.",
        ["dirty", "marker"],
        &snap.raw_stash_records_by_kind_op,
    )?;
    counter_kind_op(
        enc,
        "walshadow_raw_decode_records_total",
        "Stashed records decoded at commit resolution.",
        ["toast", "ordinary"],
        &snap.raw_decode_records_by_kind_op,
    )?;

    // Label preserves 64-bit identifier beyond float64 precision
    declare(
        enc,
        "walshadow_source_info",
        "Source cluster the pump is reading.",
        MetricType::Gauge,
    )?
    .encode_family(&[("system_id", snap.source_system_id)])?
    .encode_gauge(&1u64)?;

    encode_db_fields(&snap.by_database, enc)
}

/// Snapshot families, stage timings, then the OpenMetrics `# EOF` marker
pub fn render(snap: MetricsSnapshot) -> String {
    let mut registry = Registry::default();
    registry.register_collector(Box::new(SnapshotCollector(snap)));
    registry.register_collector(Box::new(crate::ops::stages::StageCollector));
    registry.register_collector(Box::new(crate::ops::log_events::LogEventCollector));
    let mut out = String::with_capacity(16 << 10);
    text::encode(&mut out, &registry).expect("String write cannot fail");
    out
}

/// Returns the bound address (resolves `:0` ephemeral ports) and join handle.
/// Task runs until `listener.accept` errors or the runtime tears down.
/// `status` serves `/status` off the control socket's context; `None` leaves
/// the path unserved
pub async fn serve(
    addr: SocketAddr,
    registry: MetricsRegistry,
    status: Option<crate::ops::control::SharedCtx>,
) -> Result<(SocketAddr, JoinHandle<()>), MetricsError> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| MetricsError::Bind {
            addr: addr.to_string(),
            source: e,
        })?;
    let local = listener.local_addr()?;
    let endpoints = Arc::new(Endpoints { registry, status });
    let handle = tokio::spawn(async move {
        loop {
            let (mut socket, _peer) = match listener.accept().await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(
                        target: "walshadow::metrics",
                        error = %e,
                        "accept failed; metrics server exiting",
                    );
                    return;
                }
            };
            let endpoints = endpoints.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_client(&mut socket, &endpoints).await {
                    tracing::debug!(
                        target: "walshadow::metrics",
                        error = %e,
                        "metrics client errored",
                    );
                }
                let _ = socket.shutdown().await;
            });
        }
    });
    Ok((local, handle))
}

struct Endpoints {
    registry: MetricsRegistry,
    status: Option<crate::ops::control::SharedCtx>,
}

/// Probes and scrapers send a few hundred bytes of request head
const MAX_REQUEST_HEAD: usize = 8 << 10;

/// A client that has not finished its request head by then gets no answer, so
/// a stalled connection cannot hold a task
const REQUEST_HEAD_TIMEOUT: Duration = Duration::from_secs(10);

const OPENMETRICS: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";
const TEXT: &str = "text/plain; charset=utf-8";
const JSON: &str = "application/json";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    Metrics,
    Healthz,
    Readyz,
    Status,
}

#[derive(Debug, PartialEq, Eq)]
struct Response {
    status: u16,
    content_type: &'static str,
    body: String,
}

impl Response {
    fn new(status: u16, content_type: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type,
            body: body.into(),
        }
    }

    fn text(status: u16, body: impl Into<String>) -> Self {
        Self::new(status, TEXT, body)
    }

    fn not_found() -> Self {
        Self::text(404, "not found\n")
    }

    fn reason(&self) -> &'static str {
        match self.status {
            200 => "OK",
            400 => "Bad Request",
            404 => "Not Found",
            405 => "Method Not Allowed",
            500 => "Internal Server Error",
            503 => "Service Unavailable",
            _ => "",
        }
    }

    /// HTTP/1.0, one response per connection. `HEAD` gets the headers alone,
    /// `Content-Length` included
    fn encode(&self, head_only: bool) -> Vec<u8> {
        let allow = if self.status == 405 {
            "Allow: GET, HEAD\r\n"
        } else {
            ""
        };
        let mut out = format!(
            "HTTP/1.0 {} {}\r\n\
             Content-Type: {}\r\n\
             Content-Length: {}\r\n\
             {allow}\
             Connection: close\r\n\
             \r\n",
            self.status,
            self.reason(),
            self.content_type,
            self.body.len(),
        )
        .into_bytes();
        if !head_only {
            out.extend_from_slice(self.body.as_bytes());
        }
        out
    }
}

/// Whether a request line is a `HEAD`, and the route it names or the error to
/// answer with. A query string is ignored; an unknown path is 404 whatever the
/// method, a known one 405 for anything but `GET` and `HEAD`
fn route(request_line: &str) -> (bool, Result<Route, Response>) {
    let mut parts = request_line.split_whitespace();
    let (Some(method), Some(target)) = (parts.next(), parts.next()) else {
        return (false, Err(Response::text(400, "bad request\n")));
    };
    let head_only = method == "HEAD";
    let path = target.split_once('?').map_or(target, |(p, _)| p);
    let route = match path {
        "/metrics" => Route::Metrics,
        "/healthz" => Route::Healthz,
        "/readyz" => Route::Readyz,
        "/status" => Route::Status,
        _ => return (head_only, Err(Response::not_found())),
    };
    if !matches!(method, "GET" | "HEAD") {
        return (false, Err(Response::text(405, "method not allowed\n")));
    }
    (head_only, Ok(route))
}

async fn respond(route: Route, endpoints: &Endpoints) -> Response {
    match route {
        Route::Metrics => Response::new(
            200,
            OPENMETRICS,
            render(endpoints.registry.snapshot().await),
        ),
        Route::Healthz => Response::text(200, "ok\n"),
        Route::Readyz => match endpoints.registry.health().readiness() {
            Ok(()) => Response::text(200, "ok\n"),
            Err(why) => Response::text(503, format!("not ready: {why}\n")),
        },
        Route::Status => {
            let Some(ctx) = &endpoints.status else {
                return Response::not_found();
            };
            match crate::ops::control::status_table(ctx).await {
                Ok(table) => match serde_json::to_string(&table) {
                    Ok(json) => Response::new(200, JSON, json),
                    Err(e) => Response::text(500, format!("encode status: {e}\n")),
                },
                Err(e) => Response::text(500, format!("{e:#}\n")),
            }
        }
    }
}

/// Request head up to its blank line, the size cap, or the client's EOF
async fn read_head(socket: &mut tokio::net::TcpStream) -> io::Result<Vec<u8>> {
    let mut head = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            return Ok(head);
        }
        head.extend_from_slice(&chunk[..n]);
        let ended =
            head.windows(4).any(|w| w == b"\r\n\r\n") || head.windows(2).any(|w| w == b"\n\n");
        if ended || head.len() >= MAX_REQUEST_HEAD {
            return Ok(head);
        }
    }
}

async fn handle_client(
    socket: &mut tokio::net::TcpStream,
    endpoints: &Endpoints,
) -> io::Result<()> {
    let Ok(head) = tokio::time::timeout(REQUEST_HEAD_TIMEOUT, read_head(socket)).await else {
        return Ok(());
    };
    let head = head?;
    let line = head.split(|&b| b == b'\n').next().unwrap_or_default();
    let (head_only, routed) = match std::str::from_utf8(line) {
        Ok(line) => route(line.trim_end_matches('\r')),
        Err(_) => (false, Err(Response::text(400, "bad request\n"))),
    };
    let resp = match routed {
        Ok(route) => respond(route, endpoints).await,
        Err(resp) => resp,
    };
    socket.write_all(&resp.encode(head_only)).await?;
    socket.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every family encodes through `?`, so a writer that gives out partway
    /// has to surface as an error rather than a panic or a truncated scrape
    #[test]
    fn encode_propagates_a_failing_writer_at_every_family() {
        struct FailAfter {
            writes: usize,
            out: String,
        }
        impl fmt::Write for FailAfter {
            fn write_str(&mut self, s: &str) -> fmt::Result {
                if self.writes == 0 {
                    return Err(fmt::Error);
                }
                self.writes -= 1;
                self.out.push_str(s);
                Ok(())
            }
        }

        let mut snap = MetricsSnapshot {
            crossing_blocked_on: "slot_missing",
            source_endpoint_swap_blocked_on: "slot_missing",
            promotion_blocked_on: "not_paused",
            unmapped_rows_by_table: vec![("app".into(), "public.noise".into(), 5)],
            rows_inserted_by_table: vec![("app".into(), "public.orders".into(), 9)],
            cells_inserted_by_type: vec![("String".into(), "local", 9)],
            by_database: vec![DbSeries {
                database: "app".into(),
                ..DbSeries::default()
            }],
            ..MetricsSnapshot::default()
        };
        snap.records_by_rm_route
            .insert(("Heap".into(), "to_decoder"), 3);

        let full = render(snap.clone()).len();
        for writes in 0..64 {
            let mut w = FailAfter {
                writes,
                out: String::new(),
            };
            let mut registry = Registry::default();
            registry.register_collector(Box::new(SnapshotCollector(snap.clone())));
            registry.register_collector(Box::new(crate::ops::log_events::LogEventCollector));
            let failed = text::encode(&mut w, &registry).is_err();
            assert!(failed, "prefix length {writes} must surface the failure");
            assert!(w.out.len() < full, "a failed encode cannot be complete");
        }
    }

    #[test]
    fn render_labels_every_tenant_series() {
        let out = render(MetricsSnapshot {
            tenants: vec![
                TenantMetrics {
                    id: "acme".into(),
                    dbname: "acme_db".into(),
                    phase: "active",
                    lag_bytes: 7,
                    rows_emitted: 3,
                    ..Default::default()
                },
                TenantMetrics {
                    id: "globex".into(),
                    dbname: "globex_db".into(),
                    phase: "detached",
                    ..Default::default()
                },
            ],
            ..Default::default()
        });
        assert!(out.contains(
            "walshadow_tenant_info{tenant=\"acme\",dbname=\"acme_db\",phase=\"active\"} 1"
        ));
        assert!(out.contains("walshadow_tenant_lag_bytes{tenant=\"acme\"} 7"));
        assert!(out.contains("walshadow_tenant_rows_emitted_total{tenant=\"acme\"} 3"));
        assert!(out.contains("phase=\"detached\""));
    }

    #[test]
    fn render_omits_unknown_lsn_gauges() {
        let body = render(MetricsSnapshot {
            emitter_ack_lsn: 0x47FD_FB00_0000.into(),
            ..MetricsSnapshot::default()
        });
        assert!(body.contains("walshadow_emitter_ack_lsn 79156163379200"));
        for name in [
            "walshadow_source_received_lsn",
            "walshadow_filter_lsn",
            "walshadow_shadow_replay_lsn",
            "walshadow_decoder_commit_lsn",
            "walshadow_floor_lsn",
        ] {
            assert!(!body.contains(name), "{name} rendered at LSN 0: {body}");
        }
    }

    #[test]
    fn render_includes_help_type_lines() {
        let mut snap = MetricsSnapshot {
            source_received_lsn: 0xCAFE_BABE.into(),
            filter_lsn: 0xC0FFEE.into(),
            xact_active: 3,
            uptime_seconds: 42,
            ..MetricsSnapshot::default()
        };
        snap.records_by_rm_route
            .insert(("Heap".into(), "to_decoder"), 17);

        let body = render(snap);
        assert!(body.contains("# HELP walshadow_source_received_lsn"));
        assert!(body.contains("# TYPE walshadow_source_received_lsn gauge"));
        assert!(body.contains("walshadow_source_received_lsn 3405691582"));
        assert!(body.contains("walshadow_filter_lsn 12648430"));
        assert!(
            body.contains("walshadow_filter_records_total{rmgr=\"Heap\",route=\"to_decoder\"} 17")
        );
        assert!(body.contains("walshadow_xact_active 3"));
        assert!(body.contains("walshadow_uptime_seconds_total 42"));
    }

    #[test]
    fn render_labels_per_table_and_cell_series() {
        let body = render(MetricsSnapshot::default());
        assert!(
            !body.contains("walshadow_table_rows_inserted_total{"),
            "{body}"
        );

        let body = render(MetricsSnapshot {
            cells_inserted_by_type: vec![("Decimal(38, 9)".into(), "oracle", 7)],
            rows_inserted_by_table: vec![("app".into(), "public.orders".into(), 4096)],
            skipped_records_by_rel: vec![SkippedRel {
                database: "app".into(),
                rel_node: "16400".into(),
                op: "update",
                reason: "not_covered",
                records: 3,
            }],
            ..MetricsSnapshot::default()
        });
        for want in [
            "walshadow_cells_inserted_total{type=\"Decimal(38, 9)\",encoding=\"oracle\"} 7",
            "walshadow_table_rows_inserted_total{database=\"app\",table=\"public.orders\"} 4096",
            "walshadow_decoder_skipped_records_total{database=\"app\",rel_node=\"16400\",op=\"update\",reason=\"not_covered\"} 3",
        ] {
            assert!(body.contains(want), "{want} missing from {body}");
        }
    }

    #[test]
    fn render_emits_plan_families_labelled() {
        let snap = MetricsSnapshot {
            xact_plan_rows: 9,
            xact_plan_bytes_by_storage: [100, 200],
            xact_plan_failures_by_reason: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            ..MetricsSnapshot::default()
        };
        let body = render(snap);
        assert!(body.contains("# TYPE walshadow_xact_plan_bytes counter"));
        assert!(body.contains("walshadow_xact_plan_bytes_total{storage=\"mem\"} 100"));
        assert!(body.contains("walshadow_xact_plan_bytes_total{storage=\"file\"} 200"));
        assert!(body.contains("walshadow_xact_plan_rows_total 9"));
        for (i, reason) in PLAN_FAILURE_REASONS.iter().enumerate() {
            let want = format!(
                "walshadow_xact_plan_failures_total{{reason=\"{reason}\"}} {}",
                i + 1
            );
            assert!(body.contains(&want), "{want}");
        }
    }

    /// Bootstrap stage attribution reaches `/metrics`. `BootstrapOutcome`
    /// counters used to stop at a log line, which is what made a slow
    /// initial load unattributable
    #[test]
    fn render_exposes_bootstrap_stage_attribution() {
        let snap = MetricsSnapshot {
            backfill_backup_rows_total: 7,
            backfill_backup_bytes_total: 8192,
            backfill_copy_rows_total: 3,
            backfill_copy_bytes_total: 23,
            bootstrap_bytes_tapped: 1 << 30,
            bootstrap_pages_walked: 131_072,
            bootstrap_files_skipped_unmapped: 297,
            bootstrap_decode_seconds: 12.5,
            bootstrap_channel_block_seconds: 3.25,
            bootstrap_tap_seconds: 0.0,
            inserter_ch_seconds_total: 41.5,
            oracle_resolve_seconds_total: 8.25,
            ..MetricsSnapshot::default()
        };
        let body = render(snap);
        for want in [
            "walshadow_backfill_backup_rows_total 7",
            "walshadow_backfill_backup_bytes_total 8192",
            "walshadow_backfill_copy_rows_total 3",
            "walshadow_backfill_copy_bytes_total 23",
            "walshadow_bootstrap_bytes_tapped_total 1073741824",
            "walshadow_bootstrap_pages_walked_total 131072",
            "walshadow_bootstrap_files_skipped_unmapped_total 297",
            "walshadow_bootstrap_decode_seconds_total 12.5",
            "walshadow_bootstrap_channel_block_seconds_total 3.25",
            "walshadow_bootstrap_tap_seconds_total 0.0",
            "walshadow_inserter_ch_seconds_total 41.5",
            "walshadow_oracle_resolve_seconds_total 8.25",
        ] {
            assert!(body.contains(want), "missing {want}");
        }
    }

    #[test]
    fn render_names_the_bootstrap_part_progress_pair() {
        let body = render(MetricsSnapshot {
            bootstrap_parts_total: 392,
            bootstrap_parts_done: 117,
            bootstrap_deferred_replay_bytes: 1000,
            bootstrap_deferred_replayed_bytes: 250,
            ..MetricsSnapshot::default()
        });
        for want in [
            "walshadow_bootstrap_parts_total 392",
            "walshadow_bootstrap_parts_done_total 117",
            "walshadow_bootstrap_deferred_replay_bytes 1000",
            "walshadow_bootstrap_deferred_replayed_bytes 250",
        ] {
            assert!(body.contains(want), "missing {want}\n{body}");
        }
    }

    /// Prometheus rejects a family declared twice; `descriptor_ambiguous_total`
    /// was emitted bare and labelled with contradicting HELP text
    #[test]
    fn render_declares_each_family_once() {
        let body = render(MetricsSnapshot::default());
        let mut seen: ahash::HashMap<&str, usize> = ahash::HashMap::default();
        for line in body.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ")
                && let Some(name) = rest.split_whitespace().next()
            {
                *seen.entry(name).or_default() += 1;
            }
        }
        let dupes: Vec<&&str> = seen
            .iter()
            .filter(|(_, n)| **n > 1)
            .map(|(name, _)| name)
            .collect();
        assert!(dupes.is_empty(), "families declared twice: {dupes:?}");
        let helps = body.lines().filter(|l| l.starts_with("# HELP ")).count();
        assert_eq!(helps, seen.len(), "one HELP per TYPE");
    }

    #[test]
    fn render_emits_raw_families_labelled() {
        let mut snap = MetricsSnapshot {
            raw_stash_bytes_by_storage: [11, 12],
            raw_decode_rows_by_op: [1, 2, 3, 4, 5, 6, 7],
            decoder_records_by_op: [41, 42, 43, 44, 45],
            raw_pending_rows: 13,
            raw_pending_bytes: 14,
            by_database: vec![DbSeries {
                database: "app".into(),
                descriptor_ambiguous_total: 21,
                desc_lookups_ambiguous_total: 22,
                ..DbSeries::default()
            }],
            ..MetricsSnapshot::default()
        };
        snap.raw_stash_records_by_kind_op[0][0] = 31; // dirty insert
        snap.raw_stash_records_by_kind_op[1][5] = 32; // marker multi_insert
        snap.raw_decode_records_by_kind_op[0][0] = 33; // toast insert
        snap.raw_decode_records_by_kind_op[1][2] = 34; // ordinary update
        let body = render(snap);
        assert!(
            body.contains("walshadow_raw_stash_records_total{kind=\"dirty\",op=\"insert\"} 31")
        );
        assert!(
            body.contains(
                "walshadow_raw_stash_records_total{kind=\"marker\",op=\"multi_insert\"} 32"
            )
        );
        assert!(body.contains("walshadow_raw_stash_bytes_total{storage=\"mem\"} 11"));
        assert!(body.contains("walshadow_raw_stash_bytes_total{storage=\"spill\"} 12"));
        assert!(
            body.contains("walshadow_raw_decode_records_total{kind=\"toast\",op=\"insert\"} 33")
        );
        assert!(
            body.contains("walshadow_raw_decode_records_total{kind=\"ordinary\",op=\"update\"} 34")
        );
        assert!(body.contains("walshadow_raw_decode_rows_total{op=\"multi_insert\"} 6"));
        assert!(body.contains("walshadow_decoder_records_total{op=\"hot_update\"} 43"));
        assert!(body.contains("walshadow_raw_pending_rows 13"));
        assert!(body.contains("walshadow_raw_pending_bytes 14"));
        // Published intervals and lookups landing in one stay distinct
        // families
        assert!(body.contains("walshadow_descriptor_ambiguous_total{database=\"app\"} 21"));
        assert!(body.contains("walshadow_desc_lookups_ambiguous_total{database=\"app\"} 22"));
    }

    #[test]
    fn render_keeps_each_database_on_its_own_series() {
        let body = render(MetricsSnapshot {
            by_database: vec![
                DbSeries {
                    database: "app".into(),
                    bridge_up: 1,
                    bridge_requests_by_op: [3, 0, 0, 0, 0, 0],
                    desc_log_entries: 9,
                    config_backfills_pending_by_mode: [2, 0, 0],
                    ..DbSeries::default()
                },
                DbSeries {
                    database: "billing".into(),
                    bridge_up: 0,
                    bridge_requests_by_op: [5, 0, 0, 0, 0, 0],
                    desc_log_entries: 11,
                    config_backfills_pending_by_mode: [0, 0, 7],
                    ..DbSeries::default()
                },
            ],
            ..MetricsSnapshot::default()
        });
        for want in [
            "walshadow_bridge_up{database=\"app\"} 1",
            "walshadow_bridge_up{database=\"billing\"} 0",
            &format!(
                "walshadow_bridge_requests_total{{database=\"app\",op=\"{}\"}} 3",
                OP_LABELS[0]
            ),
            &format!(
                "walshadow_bridge_requests_total{{database=\"billing\",op=\"{}\"}} 5",
                OP_LABELS[0]
            ),
            "walshadow_desc_log_entries{database=\"app\"} 9",
            "walshadow_desc_log_entries{database=\"billing\"} 11",
            "walshadow_config_backfills_pending{database=\"app\",mode=\"copy\"} 2",
            "walshadow_config_backfills_pending{database=\"billing\",mode=\"object_store\"} 7",
        ] {
            assert!(body.contains(want), "missing {want}\n{body}");
        }
        // Cluster WAL and process-wide work is counted once, unlabelled
        assert!(body.contains("walshadow_xacts_committed_total 0"), "{body}");
        assert!(body.contains("walshadow_pump_queue_depth 0"), "{body}");
    }

    /// A database name is a PG identifier, so a quote in it must not end the
    /// label and truncate the scrape
    #[test]
    fn render_escapes_a_database_label() {
        let body = render(MetricsSnapshot {
            by_database: vec![DbSeries {
                database: "we\"ird\\name".into(),
                desc_log_entries: 4,
                ..DbSeries::default()
            }],
            ..MetricsSnapshot::default()
        });
        assert!(
            body.contains(r#"walshadow_desc_log_entries{database="we\"ird\\name"} 4"#),
            "{body}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn registry_set_and_snapshot_round_trip() {
        let reg = MetricsRegistry::new();
        let snap = MetricsSnapshot {
            filter_lsn: 7.into(),
            xacts_committed_total: 11,
            ..MetricsSnapshot::default()
        };
        reg.set(snap.clone()).await;
        let got = reg.snapshot().await;
        assert_eq!(got.filter_lsn, 7);
        assert_eq!(got.xacts_committed_total, 11);
    }

    #[test]
    fn render_emits_shadow_apply_lag_lines() {
        let snap = MetricsSnapshot {
            shadow_apply_lag_bytes: 12345,
            shadow_apply_lag_seconds: 1.23456,
            shadow_stream_active_connections: 2,
            shadow_stream_dropped_connections_total: 5,
            ..MetricsSnapshot::default()
        };
        let body = render(snap);
        assert!(body.contains("# HELP walshadow_shadow_apply_lag_bytes"));
        assert!(body.contains("# TYPE walshadow_shadow_apply_lag_bytes gauge"));
        assert!(body.contains("walshadow_shadow_apply_lag_bytes 12345"));
        assert!(body.contains("# HELP walshadow_shadow_apply_lag_seconds"));
        assert!(body.contains("# TYPE walshadow_shadow_apply_lag_seconds gauge"));
        assert!(body.contains("walshadow_shadow_apply_lag_seconds 1.23456"));
        assert!(body.contains("# HELP walshadow_shadow_stream_active_connections"));
        assert!(body.contains("# TYPE walshadow_shadow_stream_active_connections gauge"));
        assert!(body.contains("walshadow_shadow_stream_active_connections 2"));
        assert!(body.contains("# HELP walshadow_shadow_stream_dropped_connections"));
        assert!(body.contains("# TYPE walshadow_shadow_stream_dropped_connections counter"));
        assert!(body.contains("walshadow_shadow_stream_dropped_connections_total 5"));
    }

    /// Unknown rate stays infinite on the wire. prometheus-client spells it
    /// `inf` where the hand-rolled encoder spelled it `+Inf`; Prometheus
    /// parses either through Go's ParseFloat
    #[test]
    fn render_emits_infinity_for_unknown_rate() {
        let snap = MetricsSnapshot {
            shadow_apply_lag_seconds: f64::INFINITY,
            ..MetricsSnapshot::default()
        };
        let body = render(snap);
        assert!(
            body.contains("walshadow_shadow_apply_lag_seconds inf"),
            "{body}"
        );
    }

    #[test]
    fn rate_estimator_rate_across_window() {
        let mut e = RateEstimator::new(Duration::from_secs(30));
        let t0 = Instant::now();
        e.observe(t0, 0);
        e.observe(t0 + Duration::from_secs(10), 10_000);
        let r = e.rate().expect("rate");
        assert!((r - 1000.0).abs() < 1e-6, "expected ~1000 B/s got {r}");
    }

    #[test]
    fn rate_estimator_prunes_outside_window() {
        let mut e = RateEstimator::new(Duration::from_secs(5));
        let t0 = Instant::now();
        e.observe(t0, 0);
        e.observe(t0 + Duration::from_secs(1), 1_000);
        e.observe(t0 + Duration::from_secs(10), 10_000);
        let (front_t, _) = *e.samples.front().unwrap();
        assert!(front_t >= t0 + Duration::from_secs(1));
    }

    #[test]
    fn rate_estimator_seconds_for_zero_lag() {
        let mut e = RateEstimator::new(Duration::from_secs(30));
        e.observe(Instant::now(), 0);
        assert_eq!(e.seconds_for(0), 0.0);
    }

    #[test]
    fn rate_estimator_seconds_for_unknown_rate_is_infinity() {
        let e = RateEstimator::new(Duration::from_secs(30));
        assert!(e.seconds_for(1024).is_infinite());
    }

    #[test]
    fn rate_estimator_seconds_for_known_rate() {
        let mut e = RateEstimator::new(Duration::from_secs(30));
        let t0 = Instant::now();
        e.observe(t0, 0);
        e.observe(t0 + Duration::from_secs(10), 10_000);
        // rate = 1000 B/s, lag 5000 B → 5 s
        let s = e.seconds_for(5_000);
        assert!((s - 5.0).abs() < 1e-6, "expected 5.0 got {s}");
    }

    /// Wire shape rather than substrings: every sample belongs to the family
    /// declared above it, counter samples carry `_total`, labels are quoted,
    /// values parse, and one EOF marker closes the body after stage families
    #[test]
    fn exposition_parses_as_openmetrics() {
        let mut snap = MetricsSnapshot {
            shadow_apply_lag_seconds: f64::INFINITY,
            uptime_seconds: 7,
            source_system_id: u64::MAX,
            by_database: vec![
                DbSeries {
                    database: "app".into(),
                    ..DbSeries::default()
                },
                DbSeries {
                    database: "billing".into(),
                    ..DbSeries::default()
                },
            ],
            ..MetricsSnapshot::default()
        };
        snap.records_by_rm_route
            .insert(("Heap".into(), "to_shadow"), 3);
        let body = render(snap);

        let (families, tail) = body.split_once("# EOF\n").expect("EOF marker");
        assert!(tail.is_empty(), "bytes after EOF: {tail:?}");
        assert!(families.contains("walshadow_stage_completed_total{stage=\"copy\"} 0"));

        let mut declared: Option<(&str, &str)> = None;
        let mut helped = "";
        for line in families.lines() {
            if let Some(rest) = line.strip_prefix("# HELP ") {
                let (name, help) = rest.split_once(' ').expect("HELP text");
                assert!(!help.is_empty(), "{name} declared with empty help");
                helped = name;
                continue;
            }
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let (name, kind) = rest.split_once(' ').expect("TYPE keyword");
                assert_eq!(name, helped, "TYPE without its own HELP: {line}");
                assert!(matches!(kind, "counter" | "gauge"), "{line}");
                declared = Some((name, kind));
                continue;
            }
            let (head, value) = line.rsplit_once(' ').expect("sample value");
            let (name, labels) = match head.split_once('{') {
                Some((name, labels)) => (name, labels.strip_suffix('}').expect("closing brace")),
                None => (head, ""),
            };
            let (family, kind) = declared.expect("sample ahead of its declaration");
            let want = match kind {
                "counter" => format!("{family}_total"),
                _ => family.to_owned(),
            };
            assert_eq!(name, want, "sample outside its family: {line}");
            for label in labels.split_terminator(',') {
                let (key, val) = label.split_once('=').expect("key=value label");
                assert!(!key.is_empty(), "{line}");
                assert!(
                    val.len() >= 2 && val.starts_with('"') && val.ends_with('"'),
                    "{line}"
                );
            }
            assert!(value.parse::<f64>().is_ok(), "unparseable value: {line}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_serve_returns_text_format_body() {
        let reg = MetricsRegistry::new();
        let snap = MetricsSnapshot {
            filter_lsn: 0xDEAD.into(),
            ..MetricsSnapshot::default()
        };
        reg.set(snap).await;
        let (addr, _handle) = serve("127.0.0.1:0".parse().unwrap(), reg, None)
            .await
            .unwrap();

        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"GET /metrics HTTP/1.0\r\n\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        sock.read_to_end(&mut buf).await.unwrap();
        let resp = String::from_utf8(buf).unwrap();
        assert!(resp.starts_with("HTTP/1.0 200 OK\r\n"), "{resp}");
        assert!(resp.contains("Content-Type: application/openmetrics-text"));
        assert!(resp.contains("walshadow_filter_lsn 57005"), "{resp}");
    }

    #[test]
    fn route_serves_known_paths_and_refuses_the_rest() {
        let ok = |line: &str| route(line).1.unwrap();
        let err = |line: &str| route(line).1.unwrap_err().status;
        assert_eq!(ok("GET /metrics HTTP/1.1"), Route::Metrics);
        assert_eq!(ok("GET /healthz HTTP/1.1"), Route::Healthz);
        assert_eq!(ok("GET /readyz HTTP/1.0"), Route::Readyz);
        assert_eq!(ok("GET /status HTTP/1.1"), Route::Status);
        assert_eq!(ok("GET /metrics?debug=1 HTTP/1.1"), Route::Metrics);
        // No HTTP version is still a request line
        assert_eq!(ok("GET /healthz"), Route::Healthz);
        assert_eq!(route("HEAD /readyz HTTP/1.1"), (true, Ok(Route::Readyz)));
        assert!(!route("GET /readyz HTTP/1.1").0);

        // The scrape used to answer on every path; now only on its own
        assert_eq!(err("GET / HTTP/1.1"), 404);
        assert_eq!(err("GET /metrics/ HTTP/1.1"), 404);
        assert_eq!(err("GET /quit HTTP/1.1"), 404);
        assert_eq!(err("POST /nope HTTP/1.1"), 404);
        assert_eq!(err("POST /metrics HTTP/1.1"), 405);
        assert_eq!(err("DELETE /healthz HTTP/1.1"), 405);
        assert_eq!(
            err("get /metrics HTTP/1.1"),
            405,
            "methods are case-sensitive"
        );
        assert_eq!(err(""), 400);
        assert_eq!(err("GET"), 400);
        // A HEAD gets no body, whatever the answer
        assert!(route("HEAD /nope HTTP/1.1").0);
    }

    #[test]
    fn encode_writes_status_headers_and_body_except_for_head() {
        let resp = Response::text(503, "not ready: x\n");
        let full = String::from_utf8(resp.encode(false)).unwrap();
        assert_eq!(
            full,
            "HTTP/1.0 503 Service Unavailable\r\n\
             Content-Type: text/plain; charset=utf-8\r\n\
             Content-Length: 13\r\n\
             Connection: close\r\n\
             \r\n\
             not ready: x\n"
        );
        let head = String::from_utf8(resp.encode(true)).unwrap();
        assert!(
            head.ends_with("Content-Length: 13\r\nConnection: close\r\n\r\n"),
            "{head}"
        );
        let refused = String::from_utf8(Response::text(405, "").encode(false)).unwrap();
        assert!(refused.contains("\r\nAllow: GET, HEAD\r\n"), "{refused}");
    }

    #[test]
    fn readiness_needs_streaming_and_the_shadow_up() {
        let health = Arc::new(Health::default());
        assert!(
            health.readiness().unwrap_err().contains("not streaming"),
            "a fresh daemon is still starting"
        );
        health.set_shadow_up(true);
        assert!(health.readiness().is_err(), "a shadow up is not enough");
        let guard = health.streaming();
        assert_eq!(health.readiness(), Ok(()));
        // The supervisor saw the postmaster stop, then brought it back
        health.set_shadow_up(false);
        assert_eq!(health.readiness(), Err("shadow postmaster is down"));
        health.set_shadow_up(true);
        assert_eq!(health.readiness(), Ok(()));
        // Leaving the pump loop, by any path, ends ready
        drop(guard);
        assert!(health.readiness().unwrap_err().contains("not streaming"));
    }

    /// One request over a fresh connection, write side closed after it, so a
    /// head without its blank line still ends: status code, headers, body
    async fn request(addr: SocketAddr, raw: &str) -> (u16, String, String) {
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        sock.write_all(raw.as_bytes()).await.unwrap();
        sock.shutdown().await.unwrap();
        let mut buf = Vec::new();
        sock.read_to_end(&mut buf).await.unwrap();
        let resp = String::from_utf8(buf).unwrap();
        let (head, body) = resp.split_once("\r\n\r\n").unwrap();
        let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
        (status, head.to_owned(), body.to_owned())
    }

    async fn get(addr: SocketAddr, path: &str) -> (u16, String) {
        let (status, _, body) =
            request(addr, &format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n")).await;
        (status, body)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn probes_answer_over_http_and_unknown_paths_do_not_scrape() {
        let reg = MetricsRegistry::new();
        let (addr, _handle) = serve("127.0.0.1:0".parse().unwrap(), reg.clone(), None)
            .await
            .unwrap();

        assert_eq!(get(addr, "/healthz").await, (200, "ok\n".into()));
        let (status, body) = get(addr, "/readyz").await;
        assert_eq!(status, 503, "{body}");
        assert!(body.starts_with("not ready: not streaming"), "{body}");

        let health = reg.health().clone();
        health.set_shadow_up(true);
        let streaming = health.streaming();
        assert_eq!(get(addr, "/readyz").await, (200, "ok\n".into()));
        health.set_shadow_up(false);
        assert_eq!(
            get(addr, "/readyz").await,
            (503, "not ready: shadow postmaster is down\n".into())
        );
        health.set_shadow_up(true);
        assert_eq!(get(addr, "/readyz").await.0, 200);
        drop(streaming);
        assert_eq!(get(addr, "/readyz").await.0, 503);

        for path in ["/", "/index.html", "/status"] {
            let (status, body) = get(addr, path).await;
            assert_eq!(status, 404, "{path}");
            assert!(
                !body.contains("walshadow_"),
                "{path} leaked the scrape: {body}"
            );
        }
        let (status, head, body) =
            request(addr, "POST /metrics HTTP/1.1\r\nContent-Length: 0\r\n\r\n").await;
        assert_eq!(status, 405);
        assert!(head.contains("Allow: GET, HEAD"), "{head}");
        assert!(!body.contains("walshadow_"));

        let (status, head, body) = request(addr, "HEAD /metrics HTTP/1.1\r\n\r\n").await;
        assert_eq!(status, 200);
        assert!(body.is_empty(), "HEAD carries no body: {body}");
        let length: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .unwrap()
            .parse()
            .unwrap();
        assert!(length > 0, "HEAD reports the body it would send");

        // A request line with no blank line after it is answered at EOF
        let (status, _, body) = request(addr, "GET /healthz").await;
        assert_eq!((status, body.as_str()), (200, "ok\n"));
        let (status, _, _) = request(addr, "\u{0}\u{1}garbage\r\n\r\n").await;
        assert_eq!(status, 400);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_serves_ctl_status_as_json_when_enabled() {
        use crate::ops::control::{Reloader, SharedCtx, fragment_path, status_table};
        let dir = tempfile::tempdir().unwrap();
        let ch_config = dir.path().join("ch-config.toml");
        std::fs::write(
            &ch_config,
            "[source]\ndbname = \"app\"\n[ch]\ndatabase = \"cdc\"\n[stream]\npaused = true\n\
             [table.public.orders]\nreplicate = true\n",
        )
        .unwrap();
        let reg = MetricsRegistry::new();
        reg.set(MetricsSnapshot {
            emitter_rows_total: 42,
            source_system_id: u64::MAX,
            ..MetricsSnapshot::default()
        })
        .await;
        let ctx = SharedCtx {
            fragment: fragment_path(&ch_config, crate::ops::control::DEFAULT_FRAGMENT).unwrap(),
            ch_config,
            cli_base: toml::Table::new(),
            metrics: reg.clone(),
            reloader: Arc::new(Reloader::default()),
            frag_lock: Arc::new(tokio::sync::Mutex::new(())),
        };
        let (addr, _handle) = serve("127.0.0.1:0".parse().unwrap(), reg, Some(ctx.clone()))
            .await
            .unwrap();

        let (status, head, body) = request(addr, "GET /status HTTP/1.1\r\n\r\n").await;
        assert_eq!(status, 200, "{body}");
        assert!(head.contains("Content-Type: application/json"), "{head}");
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["paused"], serde_json::json!(true));
        assert_eq!(json["dbname"], serde_json::json!("app"));
        assert_eq!(json["ch_database"], serde_json::json!("cdc"));
        assert_eq!(json["rows_synced"], serde_json::json!(42));
        // Kept a string, as in `ctl status`, so no JSON reader rounds it
        assert_eq!(
            json["source_system_id"],
            serde_json::json!(u64::MAX.to_string())
        );
        assert_eq!(
            json["tables"][0]["source_table"],
            serde_json::json!("app.public.orders")
        );
        // Same document `ctl status` prints, field for field
        let table = status_table(&ctx).await.unwrap();
        assert_eq!(json, serde_json::to_value(&table).unwrap());

        // A config the daemon cannot read is the status's error, not a hang
        std::fs::write(&ctx.ch_config, "not = [valid").unwrap();
        assert_eq!(get(addr, "/status").await.0, 500);
    }
}
