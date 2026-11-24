//! k16 fork: render unresolved source values as PG text for destinations
//! that take neutral values (Snowflake), attached to `ops::oracle` as its
//! `snowflake` child module. See FORK.md.

use super::*;

use crate::decode::heap_decoder::{CommittedTuple, DecodedTuple};
use crate::schema::RelDescriptor;

impl Oracle {
    /// Render only unresolved source values through the shadow's typoutput.
    /// The existing ClickHouse Native path remains independent.
    pub async fn render_text_columns(
        &self,
        tuple: &mut CommittedTuple,
        desc: &RelDescriptor,
    ) -> Result<usize, OracleError> {
        let mut cells = Vec::new();
        collect_pending(&tuple.decoded.new, false, desc, &mut cells)?;
        collect_pending(&tuple.decoded.old, true, desc, &mut cells)?;
        if cells.is_empty() {
            return Ok(0);
        }
        let size = 4usize
            + cells
                .iter()
                .map(|(_, _, oid, typmod, cell)| {
                    let _ = (oid, typmod);
                    4 + 4 + cell.wire_bytes()
                })
                .sum::<usize>();
        if size + 5 > MAX_REQUEST_BYTES {
            return Err(OracleError::Bridge(BridgeError::RequestTooLarge {
                len: size + 5,
                cap: MAX_REQUEST_BYTES,
            }));
        }
        let mut frame = request_frame(size);
        frame.extend_from_slice(&(cells.len() as u32).to_be_bytes());
        for (_, _, oid, typmod, cell) in &cells {
            frame.extend_from_slice(&oid.to_be_bytes());
            frame.extend_from_slice(&typmod.to_be_bytes());
            match cell {
                OracleCell::DiskRaw(bytes) => {
                    frame.push(CELL_DISK_RAW);
                    put_lenstr(&mut frame, bytes);
                }
                OracleCell::TextInput(bytes) => {
                    frame.push(CELL_TEXT);
                    put_lenstr(&mut frame, bytes);
                }
                _ => {
                    return Err(OracleError::Response(
                        "render_text received non-source cell".into(),
                    ));
                }
            }
        }
        let response = self.bridge(desc.rfn.db_node)?.render_text(frame).await?;
        let rendered = decode_text_response(&response, cells.len())?;
        let count = rendered.len();
        for ((old, idx, _, _, _), text) in cells.into_iter().zip(rendered) {
            let image = if old {
                tuple.decoded.old.as_mut()
            } else {
                tuple.decoded.new.as_mut()
            }
            .ok_or_else(|| OracleError::Response("tuple image vanished".into()))?;
            image.columns[idx] = Some(ColumnValue::Text(text));
        }
        Ok(count)
    }
}

fn collect_pending(
    image: &Option<DecodedTuple>,
    old: bool,
    desc: &RelDescriptor,
    out: &mut Vec<(bool, usize, u32, i32, OracleCell)>,
) -> Result<(), OracleError> {
    let Some(image) = image else {
        return Ok(());
    };
    for attr in &desc.attributes {
        if attr.dropped {
            continue;
        }
        let idx = usize::try_from(attr.attnum - 1)
            .map_err(|_| OracleError::Response(format!("bad attnum {}", attr.attnum)))?;
        let Some(Some(value)) = image.columns.get(idx) else {
            continue;
        };
        let (oid, cell) = match value {
            ColumnValue::PgPending { type_oid, raw } => {
                (*type_oid, OracleCell::DiskRaw(raw.clone()))
            }
            ColumnValue::PgPendingText { type_oid, text } => {
                (*type_oid, OracleCell::TextInput(text.as_bytes().to_vec()))
            }
            _ => continue,
        };
        if oid != attr.type_oid {
            return Err(OracleError::Response(format!(
                "column {} value OID {} differs from descriptor OID {}",
                attr.name, oid, attr.type_oid
            )));
        }
        out.push((old, idx, oid, attr.typmod, cell));
    }
    Ok(())
}

fn decode_text_response(frame: &[u8], expected: usize) -> Result<Vec<String>, OracleError> {
    let bad = |s: &str| OracleError::Response(s.into());
    if frame.len() < 5 || frame[0] != 0 {
        return Err(bad("render_text response missing success header"));
    }
    let n = u32::from_be_bytes(frame[1..5].try_into().unwrap()) as usize;
    if n != expected {
        return Err(OracleError::Response(format!(
            "render_text returned {n} cells, expected {expected}"
        )));
    }
    let mut pos = 5;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        if frame.len() - pos < 4 {
            return Err(bad("short render_text length"));
        }
        let len = u32::from_be_bytes(frame[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if len > frame.len() - pos {
            return Err(bad("short render_text value"));
        }
        out.push(
            std::str::from_utf8(&frame[pos..pos + len])
                .map_err(|_| bad("render_text returned non-UTF-8"))?
                .to_owned(),
        );
        pos += len;
    }
    if pos != frame.len() {
        return Err(bad("trailing render_text bytes"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neutral_text_response_rejects_bad_frames() {
        let mut ok = vec![0];
        ok.extend_from_slice(&1u32.to_be_bytes());
        ok.extend_from_slice(&3u32.to_be_bytes());
        ok.extend_from_slice(b"abc");
        assert_eq!(decode_text_response(&ok, 1).unwrap(), vec!["abc"]);
        assert!(decode_text_response(&ok, 2).is_err());
        let mut bad = ok.clone();
        bad.push(0);
        assert!(decode_text_response(&bad, 1).is_err());
        ok[9] = 0xff;
        assert!(decode_text_response(&ok, 1).is_err());
    }
}
