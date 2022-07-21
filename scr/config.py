class Config:
    
    def __init__(self):
        self.file_all_objects = 'datascr/all_objects.csv'
        self.file_sql_source = 'datascr/new 2.txt' 
        self.single_sql_stmt = """CREATE OR REPLACE Procedure eod_step_005
                        as
                    vBus_date   date;
                        begin
        Select MAX(LAST_WRK_DT) into vBus_date from intellect.ca850mb where BR_CD='BR0001';
        Delete SHB_TB_FISALLOC_HIST where  EOD_DATE=vBus_date;
        INSERT INTO ITSHBHO.SHB_TB_FISALLOC_HIST(ALLOC_SEQ, DEAL_FLOW, DEAL_ID, UNSOURCED_QTY, SECURITY_CODE, EOD_DATE)            
        SELECT A.ALLOC_SEQ,A.DEAL_FLOW,A.DEAL_ID,A.UNSOURCED_QTY,B.SECURITY_CODE,vBus_date
        FROM   INTELLECT.TB_FIS_DEAL_ALLOC A
        INNER  JOIN INTELLECT.VW_FISDEAL B ON A.DEAL_ID = B.DEAL_ID
        WHERE  TRIM(B.DEAL_STATUS) = N'DEAL_COMPLETED'
             OR TRIM(B.DEAL_STATUS) = N'BO_AUTHORIZED'
        ORDER  BY ALLOC_SEQ;    
        commit;            
end;  """