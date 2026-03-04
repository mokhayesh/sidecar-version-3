
import snowflake.connector
def get_snowflake_connection(cfg):
    return snowflake.connector.connect(
        account=cfg.get("sf_account"),
        user=cfg.get("sf_user"),
        password=cfg.get("sf_password"),
        warehouse=cfg.get("sf_warehouse"),
        database=cfg.get("sf_database"),
        schema=cfg.get("sf_schema"),
        role=cfg.get("sf_role")
    )
