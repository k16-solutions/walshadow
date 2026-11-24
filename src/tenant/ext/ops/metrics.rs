//! k16 fork: `tenant=` labelled series, attached to `ops::metrics` as its
//! `tenant` child module. See FORK.md.

use super::*;

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

pub(super) fn encode_tenants(
    tenants: &[TenantMetrics],
    enc: &mut DescriptorEncoder<'_>,
) -> fmt::Result {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
