-------------------------------------------------
-- Begin APS data processing (premium) --
-------------------------------------------------
DECLARE @bat_status char(1) 
DECLARE @num_co varchar(50); SET @num_co= 'XXX
DECLARE @cycle_name varchar(100)   
DECLARE @d_begin datetime 
DECLARE @d_end datetime 
DECLARE @d_inserted_em         datetime 
DECLARE @d_inserted datetime 
DECLARE @id_bat int 
DECLARE @send_emails         char(1) 

SET @cycle_name = 'XXX APS Processing'
SET @send_emails = 'y' 

EXEC sp_get_new_batch @num_co,@cycle_name,'p',@d_begin OUTPUT,@d_end OUTPUT,@id_bat OUTPUT

SET @d_end = '2099-12-31'

SELECT @cycle_name as cycle, @d_begin as process_date_to_begin,@d_end as process_date_to_end

EXEC [dbo].[sp_ld_fact_prem_fm_APS] @d_begin,@d_end
