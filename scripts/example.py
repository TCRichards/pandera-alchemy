import datetime as dt

from pandera_alchemy.common_tables import get_cell_group_summary_report_table
from pandera_alchemy.redshift import create_rs_engine, create_rs_rc_connection

if __name__ == "__main__":
    engine = create_rs_engine()
    cg_summary_table = get_cell_group_summary_report_table(dt.date(2024, 6, 10), "powin")
    cg_summary_table.validate(engine, check_nullable=False)

    connector = create_rs_rc_connection()
    cg_summary_table.validate(connector, check_nullable=False)
