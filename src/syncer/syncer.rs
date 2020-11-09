use crate::DbConnection;

pub struct Syncer<'a> {
    db_conn: DbConnection<'a>,
}
