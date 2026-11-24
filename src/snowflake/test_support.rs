//! Relation fixtures for the Snowflake extensions' unit tests. Upstream test
//! modules keep their fixtures private, so fork tests build theirs here.

use crate::schema::{RelAttr, RelDescriptor, RelName, ReplIdent};
use walrus::pg::walparser::RelFileNode;

/// A live (not dropped), nullable attribute with default storage
pub(crate) fn attr(attnum: i16, name: &str, type_oid: u32) -> RelAttr {
    RelAttr {
        attnum,
        name: name.into(),
        type_oid,
        typmod: -1,
        not_null: false,
        dropped: false,
        type_name: String::new(),
        type_byval: true,
        type_len: 8,
        type_align: 'd',
        type_storage: 'p',
        missing_default: None,
    }
}

/// `public.<name>` with a fixed relfilenode and oid
pub(crate) fn desc(name: &str, attrs: Vec<RelAttr>, pk: Option<Vec<i16>>) -> RelDescriptor {
    RelDescriptor {
        rfn: RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node: 16400,
        },
        oid: 16400,
        toast_oid: 0,
        namespace_oid: 2200,
        rel_name: RelName::new("public", name),
        kind: 'r',
        persistence: 'p',
        ancestors: Vec::new(),
        replident: ReplIdent::Default { pk_attnums: pk },
        attributes: attrs,
    }
}
