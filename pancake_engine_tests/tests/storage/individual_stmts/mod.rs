pub mod primary;
pub mod secondary;

use super::helpers::one_stmt::OneStmtDbAdaptor;
use anyhow::Result;

pub async fn test_stmts_serially(db: &mut impl OneStmtDbAdaptor) -> Result<()> {
    primary::put_del_get_getrange(db).await?;
    primary::nonexistent(db).await?;
    primary::zero_byte_value(db).await?;
    primary::tuple(db).await?;

    secondary::whole::delete_create_get(db).await?;
    secondary::partial::delete_create_get(db).await?;

    Ok(())
}
