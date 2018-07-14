import cx_Oracle
from check_set import CS


class DBManager(object):
    def __init__(self, connection_string):
        self.connection_string = connection_string

    def get_cs(self):
        query = '''
                SELECT * FROM (
                    SELECT
                        v_st.*,
                        row_number()
                            OVER (
                                PARTITION BY SRC_CODE, PKG_ID, TGT_LAYER, PERIOD
                                ORDER BY INSERT_DATE )rn
                    FROM (
                        select
                            CS_ID,
                            ds.SRC_CODE as SRC_CODE,
                           ds.PKG_ID,
                           ds.TGT_LAYER as TGT_LAYER,
                           TO_CHAR(ds.PERIOD,'yyyymmddhh24miss') as PERIOD,
                           dcs.CS_STATE as CS_STATE,
                           dcs.notify_flag as notify_flag,
                           dcs.TYPE_LOAD as TYPE_LOAD,
                           dcs.STATE_ID as STATE_ID,
                           dcs.INSERT_DATE as INSERT_DATE
                          ,row_number() over (partition BY CS_ID order by dcs.INSERT_DATE DESC) AS r_n
                        from DQ_CS ds
                          join DQ_CS_STATE dcs
                            using(CS_ID)) v_st
                    WHERE CS_STATE = 'CHECKED' and notify_flag='N'
                    )rn
                WHERE rn = 1
                '''
        db = None
        cursor = None

        try:
            db = cx_Oracle.connect(self.connection_string)
            cursor = db.cursor()
            data = []
            for row in cursor.execute(query):
                data.append(CS(row))
            return data
        except cx_Oracle.DatabaseError as e:
            print("Database connection error: %s", e)
        finally:
            if cursor is not None:
                cursor.close()
            if db is not None:
                db.close()

    def update_flag(self, cs):
        query = """
                    update dq_cs_state
                    set notify_flag='Y'
                    where cs_id in (select cs_id
                                    from dq_cs c
                                    where
                                        c.src_code='%s' and
                                        c.pkg_id=%s and
                                        c.tgt_layer='%s' and
                                        c.period=to_date('%s','yyyymmddhh24miss'))
                """ % (cs.src_code, str(cs.pkg_id), cs.tgt_layer, cs.period)
        db = None
        cursor = None
        try:
            db = cx_Oracle.connect(self.connection_string)
            cursor = db.cursor()
            cursor.execute(query)
            db.commit()
        except cx_Oracle.DatabaseError as e:
            print("Database connection error: %s", e)
        finally:
            if cursor is not None:
                cursor.close()
            if db is not None:
                db.close()