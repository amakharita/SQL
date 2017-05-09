USE [XXX]
GO
/****** Object:  StoredProcedure [dbo].[sp_ld_XXX_fm_APS]    Script Date: 2/27/2017 8:26:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

--***************************************************************************
-- Process name: [dbo].[sp_ld_XXX_fm_APS]
-- Purpose:      Load XXX from Stamps_ITDv2 table (s: Drive in Stamp Collector folder)
-- Author:       Adam Makharita
-- Created:      09/28/2016
--***************************************************************************
-- Parameters: @d_begin - Starting date to get data from XXX
--             @d_end - Ending date to get data from [dbo].[XXX]
--***************************************************************************


ALTER PROCEDURE [dbo].[sp_ld_XXX_fm_APS]
   (
    @d_begin datetime -- Starting date to pull from [XXX] table
   ,@d_end   datetime -- Ending date to pull from [XXX] table
   )
AS
SET NOCOUNT ON

--XXX variables
DECLARE @XXX
DECLARE @id_rowcol int
DECLARE @id_sc int
DECLARE @account_period datetime
DECLARE @date_processed datetime
DECLARE @source_file varchar(200)
DECLARE @policy_and_line_types varchar(200)
DECLARE @policy_number varchar(200)
DECLARE @effective_date datetime
DECLARE @expiration_date datetime
DECLARE @transaction_code varchar(200)
DECLARE @policy_limit decimal(30,2)
DECLARE @agency_commission_amount decimal(30,2)
DECLARE @transaction_amount decimal(30,2)

-- XXX variables
DECLARE @lPolicyLiabilityKey as int
DECLARE @pol_num as varchar(20)
DECLARE @trans as varchar(30)
DECLARE @vers as int
DECLARE @d_book as datetime
DECLARE @d_pol_eff as datetime
DECLARE @d_pol_exp as datetime
DECLARE @d_tran_eff as datetime
DECLARE @d_tran_exp as datetime
DECLARE @d_tran_proc as datetime
DECLARE @num_co as varchar(10)
DECLARE @pol_num_legacy as varchar(50)
DECLARE @rpt_co_name as varchar(50)
--DECLARE @co_name as varchar(50) -- Declared/Set in job parameters below
DECLARE @underwriter as varchar(500)
DECLARE @producer_code as varchar(20)
DECLARE @producer_name as varchar(500)
DECLARE @aslob as varchar(5)
DECLARE @subline as varchar(5)
DECLARE @class as varchar(5)
DECLARE @cvg_desc as varchar(100)
DECLARE @product_cd as varchar(10)
DECLARE @product_grp_cd as varchar(5)
DECLARE @insd_name as varchar(500)
DECLARE @insd_addr as varchar(251)
DECLARE @insd_city as varchar(50)
DECLARE @insd_state as varchar(6)
DECLARE @insd_zip as varchar(10)
DECLARE @num_loc as int
DECLARE @pymt_term as varchar(50)
DECLARE @lmt_type as varchar(20)
DECLARE @lmt_cvg_s as decimal(38, 11)
DECLARE @lmt_pol_s as decimal(18, 2)
DECLARE @cvg_attach_point as decimal(18, 2)
DECLARE @pol_attach_point as decimal(18, 2)
DECLARE @wp_s_pct as decimal(18, 8)
DECLARE @reins_retained_pct as decimal(18, 8)
DECLARE @fac_reins as varchar(1)
DECLARE @reins_special as varchar(1)
DECLARE @reins_type as varchar(50)
DECLARE @reins_nonoblig_type as varchar(100)
DECLARE @reins_nonoblig_pgm_type as varchar(100)
DECLARE @reins_nonoblig_ces_rate as decimal(18, 2)
DECLARE @amt_type as varchar(30)
DECLARE @amt_desc as varchar(30)
DECLARE @amt as decimal(18, 2)
DECLARE @ADD_DED_pct as decimal(7, 4)
DECLARE @d_updated as datetime
DECLARE @f_clsd as varchar(1)
DECLARE @cvg_state as varchar(6)
DECLARE @product as varchar(50)
DECLARE @sub_product as varchar(50)

---- Edit variables
--DECLARE @edit_verify	int;SET @edit_verify = 1 -- 0 = pass, 1 = fail
DECLARE @err_warn varchar(1);SET @err_warn = null
DECLARE @err_msg varchar(1000);SET @err_msg = null
DECLARE @initial_pol_eff DATETIME; SET @initial_pol_eff = NULL
DECLARE @initial_pol_exp DATETIME; SET @initial_pol_exp = NULL
DECLARE @new_pol INT; SET @new_pol = NULL
DECLARE @d_pol_eff_valid INT; SET @d_pol_eff_valid = NULL
DECLARE @d_pol_exp_valid INT; SET @d_pol_exp_valid = NULL
DECLARE @err_cnt INT; SET @err_cnt = 0
DECLARE @implied_terrorism_flag VARCHAR(1); SET @implied_terrorism_flag = 'N'
DECLARE @prev_id_rowcol int; SET @prev_id_rowcol = 0

--Set job parameters
DECLARE @co_name varchar(50); SET @co_name = 'Berkley Asset Protection Underwriters'
DECLARE @proc_name varchar(100); SET @proc_name = 'sp_ld_XXX_fm_APS'
DECLARE @cycle_name varchar(100); SET @cycle_name = 'XXX APS Processing'
DECLARE @id_job int; SET @id_job = 1010
DECLARE @data_source varchar(10); SET @data_source = 'APS'
DECLARE @d_inserted datetime; SET @d_inserted = GETDATE()

--Set batch number and dates
DECLARE @bat_status		char(1)										
DECLARE @d_bat_start	datetime									
DECLARE @d_bat_end		datetime 
DECLARE @d_inserted_em	datetime
DECLARE @id_bat			int
EXECUTE sp_get_updt_batch_info '062',@cycle_name,'G: ' 
			,@bat_status OUTPUT	
			,@d_bat_start OUTPUT
			,@d_bat_end OUTPUT	
			,@d_inserted_em OUTPUT		 
			,@id_bat OUTPUT	

--Remove timestamps (prevents 00:00:00.000 timestamp from restricting data on the final day of the month)			
DECLARE @d_first date; SET @d_first = CAST(@d_bat_start as DATE)
DECLARE @d_last date;  SET @d_last = CAST(@d_bat_end as DATE)

-- Clear error_msgs and XXX for all data loaded in specified date range
DELETE FROM error_msgs WHERE proc_name = @proc_name AND id_bat = @id_bat 
DELETE FROM XXX where d_book between @d_first and @d_last and data_source = @data_source

SET @cXXXXXX = CURSOR FOR
---- Create premium record from each row
select -- Pull all fields directly from XXX for edits/validation
       g.rowcol
	  ,g.id_sc
	  ,g.account_period
	  ,g.date_processed
	  ,g.source_file
	  ,g.Policy_Number
	  ,g.Effective_Date
   	  ,g.Expiration_Date
	  ,g.Transaction_Code
	  ,g.Policy_Limit
	  ,g.Agency_Commission_Amount
	  ,g.Transaction_Amount


      --***************************************************
      -- Map XXX data into XXX format
      --***************************************************
      ,g.id_sc as lPolicyLiabilityKey
      ,g.policy_number as policy_number
      ,g.transaction_code as trans
      ,0 as vers
      ,g.account_period as d_book
      ,g.Effective_Date as d_pol_eff
      ,g.expiration_date as d_pol_exp
      ,g.date_processed as d_tran_eff
      ,g.expiration_date as d_tran_exp
      ,g.date_processed as d_tran_proc
      ,'XXX' as num_co
      ,NULL as pol_num_legacy
	  ,'XXX' as rpt_co_name
      --,@co_name as co_name -- Already Set, Does not need to be reset in each cursor loop.
      ,'XXX' as underwriter
      ,'XXX' as producer_code
      ,'XXX.' as producer_name
      ,'XXX' as aslob
      ,'N/A' as subline
      ,'XXX' as class
      ,'APS' as cvg_desc
      ,'XXX' as product_cd
      ,'XXX' as product_grp_cd
      ,account_name  as insd_name
      ,address as insd_addr
      ,city as insd_city
      ,statecode as insd_state
      ,left(cast(postalcode as int), 5) as insd_zip
      ,1 as num_loc
      ,'Single Payment' as pymt_term
      ,'Property' as lmt_type
      ,g.policy_limit as lmt_cvg_s
      ,g.policy_limit as lmt_pol_s
      ,0 as cvg_attach_point 
      ,0 as pol_attach_point 
      ,100.00 as wp_s_pct
      ,100.00 as reins_retained_pct
      ,'N' as fac_reins
      ,'N' as reins_special
      ,'N/A' as reins_type
      ,'N/A' as reins_nonoblig_type
      ,'N/A' as reins_nonoblig_pgm_type
      ,0.00 as reins_nonoblig_ces_rate
      --,@data_source as data_source -- Already Set, Does NOT need to be reset in each cursor loop.
      ,'Premium' as amt_type
      ,'Written Premium' as amt_desc
      ,g.Transaction_Amount as amt
      ,0.0000 as add_ded_pct
      ,NULL as d_updated
      --,@d_inserted as d_inserted -- Feed date? Need to be updated? -- Already Set, Does NOT need to be reset in each cursor loop.
      --,@id_job as id_job -- 1004 next job sequence available -- Already Set, Does NOT need to be reset in each cursor loop.
      --,@id_bat as id_bat -- Already Set, Does NOT need to be reset in each cursor loop.
      ,NULL as f_clsd
      ,StateCode as cvg_state
      ,'XXX' as product
      ,'Stamp Collectors' as sub_product
from XXX.dbo.XXX g
where CAST(g.account_period as DATE) between @d_first and @d_last

UNION ALL

-- Create commission record from each row
select -- Pull all fields directly from XXX for edits/validation
       g.rowcol
	  ,g.id_sc
	  ,g.account_period
	  ,g.date_processed
	  ,g.source_file
	  ,g.Policy_Number
	  ,g.Effective_Date
   	  ,g.Expiration_Date
	  ,g.Transaction_Code
	  ,g.Policy_Limit
	  ,g.Agency_Commission_Amount
	  ,g.Transaction_Amount
      --***************************************************
      -- Map XXX_Stamp_Collecotrs_ITD data into XXX format
      --***************************************************
      ,g.id_sc as lPolicyLiabilityKey
      ,g.policy_number as policy_number
      ,g.Transaction_Code as trans
      ,0 as vers
      ,g.account_period as d_book
      ,g.effective_date as d_pol_eff
      ,g.expiration_date as d_pol_exp
      ,g.date_processed as d_tran_eff
      ,g.Expiration_Date as d_tran_exp
      ,g.date_processed as d_tran_proc
      ,'XXX' as num_co
      ,NULL as pol_num_legacy
      ,'XXX' as rpt_co_name
      --,@co_name as co_name -- Already Set, Does not need to be reset in each cursor loop.
      ,'XXX' as underwriter
      ,'XXX' as producer_code
      ,'XXX.' as producer_name
      ,'XXX' as aslob
      ,'N/A' as subline
      ,'XXX' as class
      ,'APS' as cvg_desc
      ,'APSP' as product_cd
      ,'FA' as product_grp_cd
      ,account_name  as insd_name
      ,address as insd_addr
      ,city as insd_city
      ,statecode as insd_state
      ,left(cast(postalcode as int), 5) as insd_zip
      ,1 as num_loc
      ,'Single Payment' as pymt_term
      ,'Property' as lmt_type
      ,g.policy_limit as lmt_cvg_s
      ,g.policy_limit as lmt_pol_s
      ,0 as cvg_attach_point 
      ,0 as pol_attach_point 
      ,100.00 as wp_s_pct
      ,100.00 as reins_retained_pct
      ,'N' as fac_reins
      ,'N' as reins_special
      ,'N/A' as reins_type
      ,'N/A' as reins_nonoblig_type
      ,'N/A' as reins_nonoblig_pgm_type
      ,0.00 as reins_nonoblig_ces_rate
      --,@data_source as data_source -- Already Set, Does NOT need to be reset in each cursor loop.
      ,'Add_Ded' as amt_type
      ,'Commission' as amt_desc
      ,g.Agency_Commission_Amount as amt
      ,case 
          when Transaction_Amount = 0 THEN 0.0000
          when Transaction_Amount IS NULL THEN 0.0000
          else CAST(agency_commission_amount/Transaction_Amount * 100 as DECIMAL(18,4)) 
       end as add_ded_pct
      ,NULL as d_updated
      --,@d_inserted as d_inserted -- Feed date? Need to be updated? -- Already Set, Does NOT need to be reset in each cursor loop.
      --,@id_job as id_job -- 1004 next job sequence available -- Already Set, Does NOT need to be reset in each cursor loop.
      --,@id_bat as id_bat -- Already Set, Does NOT need to be reset in each cursor loop.
      ,NULL as f_clsd
      ,statecode as cvg_state
      ,'XXX' as product
      ,'Stamp Collectors' as sub_product
from XXX.dbo.XXX g
where CAST(g.account_period as DATE) between @d_first and @d_last
order by g.RowCol


OPEN @cXXXXXX

if @@ERROR <> 0
BEGIN
   print 'Unable to read data from XXX.dbo.XXX'
   RETURN -1
END

FETCH NEXT
FROM @cXXXXXX
INTO @id_rowcol
	,@id_sc
    ,@account_period
    ,@date_processed
    ,@source_file
    ,@policy_number
    ,@effective_date
    ,@expiration_date
    ,@transaction_code
	,@policy_limit
    ,@agency_commission_amount
	,@transaction_amount
    ,@lPolicyLiabilityKey
    ,@pol_num
    ,@trans
    ,@vers
    ,@d_book
    ,@d_pol_eff
    ,@d_pol_exp
    ,@d_tran_eff
    ,@d_tran_exp
    ,@d_tran_proc
    ,@num_co
    ,@pol_num_legacy
    ,@rpt_co_name
    ,@underwriter
    ,@producer_code
    ,@producer_name
    ,@aslob
    ,@subline
    ,@class
    ,@cvg_desc
    ,@product_cd
    ,@product_grp_cd
    ,@insd_name
    ,@insd_addr
    ,@insd_city
    ,@insd_state
    ,@insd_zip
    ,@num_loc
    ,@pymt_term
    ,@lmt_type
    ,@lmt_cvg_s
    ,@lmt_pol_s
    ,@cvg_attach_point
    ,@pol_attach_point
    ,@wp_s_pct
    ,@reins_retained_pct
    ,@fac_reins
    ,@reins_special
    ,@reins_type
    ,@reins_nonoblig_type
    ,@reins_nonoblig_pgm_type
    ,@reins_nonoblig_ces_rate
    ,@amt_type
    ,@amt_desc
    ,@amt
    ,@ADD_DED_pct
    ,@d_updated
    ,@f_clsd
    ,@cvg_state
    ,@product
    ,@sub_product

IF @@ERROR <> 0
BEGIN
   print 'Unable to FETCH (Initial Fetch) data from cursor @cXXX_stampcollectors_ITD'
   RETURN -2
END

WHILE (@@FETCH_STATUS = 0)
BEGIN

   ---*** Population Edits ***---
   IF @id_rowcol <> @prev_id_rowcol --ensure edit errors are only sent once per row, each id_rowcol creates 2 rows - 1 premium, 1 commission
   BEGIN
   
	   -- id_sc
	   IF @id_sc is NULL
	   BEGIN
		  SET @err_msg = 'APS001: Error - APS - id_sc NOT populated: policy_number = ' + @pol_num + '.'
	      
		  EXEC sp_log_errors @num = @pol_num
							,@num_cd = 'P'
							,@co_name = @co_name
							,@cycle_name = @cycle_name
							,@proc_name = @proc_name
							,@err_msg = @err_msg
							,@err_msg_cd = 'E'
							,@sql_status = @@ERROR
		  SET @err_warn = NULL
		  SET @err_msg = NULL
		  SET @err_cnt = @err_cnt + 1
	   END
	   
	   -- account_period
	   IF @account_period is NULL
	   BEGIN
		  SET @err_msg = 'APS002: Error - APS - Account Period NOT populated: policy_number = ' + @pol_num 
					   + '. id_gp = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                   
		  EXEC sp_log_errors @num = @pol_num
							,@num_cd = 'P'
							,@co_name = @co_name
							,@cycle_name = @cycle_name
							,@proc_name = @proc_name
							,@err_msg = @err_msg
							,@err_msg_cd = 'E'
							,@sql_status = @@ERROR
		  SET @err_warn = NULL
		  SET @err_msg = NULL
		  SET @err_cnt = @err_cnt + 1
	   END
	   ELSE IF ISDATE(@account_period) = 0
	   BEGIN
		  SET @err_msg = 'APS003: Error - APS - Account Period NOT in valid date format: account_period: ' + CONVERT(VARCHAR(10),@account_period,121) 
					   + '. policy_number = ' + @pol_num + '. id_sc = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                   
		  EXEC sp_log_errors @num = @pol_num
							,@num_cd = 'P'
							,@co_name = @co_name
							,@cycle_name = @cycle_name
							,@proc_name = @proc_name
							,@err_msg = @err_msg
							,@err_msg_cd = 'E'
							,@sql_status = @@ERROR
		  SET @err_warn = NULL
		  SET @err_msg = NULL
		  SET @err_cnt = @err_cnt + 1
	   END
	   
	   -- date_processed
	   IF @date_processed is NULL
	   BEGIN
		  SET @err_msg = 'APS004: Error - APS - date_processed NOT populated: policy_number = ' + @pol_num 
					   + '. id_sc = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                   
		  EXEC sp_log_errors @num = @pol_num
							,@num_cd = 'P'
							,@co_name = @co_name
							,@cycle_name = @cycle_name
							,@proc_name = @proc_name
							,@err_msg = @err_msg
							,@err_msg_cd = 'E'
							,@sql_status = @@ERROR
		  SET @err_warn = NULL
		  SET @err_msg = NULL
		  SET @err_cnt = @err_cnt + 1
	   END
	   ELSE IF ISDATE(@date_processed) = 0
	   BEGIN
		  SET @err_msg = 'APS005: Error - APS - date_processed NOT in valid date format: date_processed: ' + CONVERT(VARCHAR(10),@account_period,121) 
					   + '. policy_number = ' + @pol_num + '. id_sc = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                   
		  EXEC sp_log_errors @num = @pol_num
							,@num_cd = 'P'
							,@co_name = @co_name
							,@cycle_name = @cycle_name
							,@proc_name = @proc_name
							,@err_msg = @err_msg
							,@err_msg_cd = 'E'
							,@sql_status = @@ERROR
		  SET @err_warn = NULL
		  SET @err_msg = NULL
		  SET @err_cnt = @err_cnt + 1
	   END
	   
	   --source_file
	   IF @source_file is NULL
	   BEGIN
		  SET @err_msg = 'APS006: Error - APS - source_file NOT populated: policy_number = ' + @pol_num 
					   + '. id_sc = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                   
		  EXEC sp_log_errors @num = @pol_num
							,@num_cd = 'P'
							,@co_name = @co_name
							,@cycle_name = @cycle_name
							,@proc_name = @proc_name
							,@err_msg = @err_msg
							,@err_msg_cd = 'E'
							,@sql_status = @@ERROR
		  SET @err_warn = NULL
		  SET @err_msg = NULL
		  SET @err_cnt = @err_cnt + 1
	   END
	   
	   --policy_number
	   IF @policy_number is NULL
	   BEGIN
		  SET @err_msg = 'APS007: Error - APS - Policy Number NOT populated: policy_number = ' + @policy_number
					   + '. id_sc = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                   
		  EXEC sp_log_errors @num = @pol_num
							,@num_cd = 'P'
							,@co_name = @co_name
							,@cycle_name = @cycle_name
							,@proc_name = @proc_name
							,@err_msg = @err_msg
							,@err_msg_cd = 'E'
							,@sql_status = @@ERROR
		  SET @err_warn = NULL
		  SET @err_msg = NULL
		  SET @err_cnt = @err_cnt + 1
	   END
	   
	   -- policy_effective_date
	   IF @effective_date is NULL
	   BEGIN
		  SET @err_msg = 'APS008: Error - APS - policy_effective_date NOT populated: policy_number = ' + @policy_number 
					   + '. id_sc = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                   
		  EXEC sp_log_errors @num = @pol_num
							,@num_cd = 'P'
							,@co_name = @co_name
							,@cycle_name = @cycle_name
							,@proc_name = @proc_name
							,@err_msg = @err_msg
							,@err_msg_cd = 'E'
							,@sql_status = @@ERROR
		  SET @err_warn = NULL
		  SET @err_msg = NULL
		  SET @err_cnt = @err_cnt + 1
	   END
	   ELSE IF ISDATE(@effective_date) = 0
	   BEGIN
		  SET @err_msg = 'APS011: Error - APS - effective_date NOT in valid date format: effective_date: ' + CONVERT(VARCHAR(10),@effective_date,121) 
					   + '. policy_number = ' + @policy_number 
					   + '. id_sc = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                   
		  EXEC sp_log_errors @num = @pol_num
							,@num_cd = 'P'
							,@co_name = @co_name
							,@cycle_name = @cycle_name
							,@proc_name = @proc_name
							,@err_msg = @err_msg
							,@err_msg_cd = 'E'
							,@sql_status = @@ERROR
		  SET @err_warn = NULL
		  SET @err_msg = NULL
		  SET @err_cnt = @err_cnt + 1
	   END

	   -- policy_expiration_date
	   IF @expiration_date is NULL
	   BEGIN
		  SET @err_msg = 'APS012: Error - APS - expiration_date NOT populated: policy_number = ' + @policy_number 
					   + '. id_gp = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                   
		  EXEC sp_log_errors @num = @pol_num
							,@num_cd = 'P'
							,@co_name = @co_name
							,@cycle_name = @cycle_name
							,@proc_name = @proc_name
							,@err_msg = @err_msg
							,@err_msg_cd = 'E'
							,@sql_status = @@ERROR
		  SET @err_warn = NULL
		  SET @err_msg = NULL
		  SET @err_cnt = @err_cnt + 1
	   END
	   ELSE IF ISDATE(@expiration_date) = 0
	   BEGIN
		  SET @err_msg = 'APS013: Error - APS - policy_expiration_date NOT in valid date format:expiration_date: ' + CONVERT(VARCHAR(10),@expiration_date,121) 
					   + '. policy_number = ' + @policy_number 
					   + '. id_sc = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                   
		  EXEC sp_log_errors @num = @pol_num
							,@num_cd = 'P'
							,@co_name = @co_name
							,@cycle_name = @cycle_name
							,@proc_name = @proc_name
							,@err_msg = @err_msg
							,@err_msg_cd = 'E'
							,@sql_status = @@ERROR
		  SET @err_warn = NULL
		  SET @err_msg = NULL
		  SET @err_cnt = @err_cnt + 1
	   END
	   
	   -- transaction_type
	   IF @transaction_code is NULL
	   BEGIN
		  SET @err_msg = 'APS014: Error - APS - transaction_type NOT populated: policy_number = ' + @policy_number 
					   + '. id_gp = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                   
		  EXEC sp_log_errors @num = @pol_num
							,@num_cd = 'P'
							,@co_name = @co_name
							,@cycle_name = @cycle_name
							,@proc_name = @proc_name
							,@err_msg = @err_msg
							,@err_msg_cd = 'E'
							,@sql_status = @@ERROR
		  SET @err_warn = NULL
		  SET @err_msg = NULL
		  SET @err_cnt = @err_cnt + 1
	   END
	   

	   ELSE IF ISNUMERIC(@transaction_amount) = 0
	   BEGIN
		  SET @err_msg = 'APS022: Error - APS - premium NOT in numeric format: premium: ' + CAST(@transaction_amount as varchar(20)) 
					   + '. policy_number = ' + @policy_number 
					   + '. id_gp = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                   
		  EXEC sp_log_errors @num = @pol_num
							,@num_cd = 'P'
							,@co_name = @co_name
							,@cycle_name = @cycle_name
							,@proc_name = @proc_name
							,@err_msg = @err_msg
							,@err_msg_cd = 'E'
							,@sql_status = @@ERROR
		  SET @err_warn = NULL
		  SET @err_msg = NULL
		  SET @err_cnt = @err_cnt + 1  
	   END
	   
	   
	   --agent_commission
	   IF @agency_commission_amount IS NULL
	   BEGIN
		  SET @err_msg = 'APS025: Error - APS - agency_commission_amount NOT populated: policy_number = ' + @policy_number 
					   + '. id_sc = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                   
		  EXEC sp_log_errors @num = @pol_num
							,@num_cd = 'P'
							,@co_name = @co_name
							,@cycle_name = @cycle_name
							,@proc_name = @proc_name
							,@err_msg = @err_msg
							,@err_msg_cd = 'E'
							,@sql_status = @@ERROR
		  SET @err_warn = NULL
		  SET @err_msg = NULL
		  SET @err_cnt = @err_cnt + 1 
	   END
	   ELSE IF ISNUMERIC(@agency_commission_amount) = 0
	   BEGIN
		  SET @err_msg = 'APS026: Error - APS - agent_commission NOT in numeric format: agency_commission_amount: ' + CAST(@agency_commission_amount as varchar(20)) 
					   + '. policy_number = ' + @policy_number 
					   + '. id_sc = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                   
		  EXEC sp_log_errors @num = @pol_num
							,@num_cd = 'P'
							,@co_name = @co_name
							,@cycle_name = @cycle_name
							,@proc_name = @proc_name
							,@err_msg = @err_msg
							,@err_msg_cd = 'E'
							,@sql_status = @@ERROR
		  SET @err_warn = NULL
		  SET @err_msg = NULL
		  SET @err_cnt = @err_cnt + 1  
	   END
	   
     

	   --*** Value Validation Edits **--
	  
	   -- Determine if this policy is currently in XXX
	   select @new_pol = COUNT(*)
	   from XXX.dbo.XXX
	   where pol_num = @pol_num
	   
	   --print @new_pol
	   --print @d_pol_eff
	   --print @d_pol_exp
	  
	   IF @new_pol <> 0
	   BEGIN
	  
		  -- Find initial policy effective and expiration dates in XXX
		  select @initial_pol_eff = d_pol_eff
				,@initial_pol_exp = d_pol_exp
		  from XXX.dbo.XXX fp WITH (NOLOCK)
		  Inner JOIN
			 (select MIN(lPolicyLiabilityKey) lPolicyLiabilityKey 
			  from XXX.dbo.XXX WITH (NOLOCK) 
			  where pol_num = @pol_num) fp_min
			ON fp.lPolicyLiabilityKey = fp_min.lPolicyLiabilityKey
		  where pol_num = @pol_num
			and data_source = @data_source
	     
		  -- Determine if @d_pol_eff (pol effective date) and @d_pol_exp (pol expiration date) in XXX range
		  select @d_pol_eff_valid = CASE WHEN CAST(@d_pol_eff as DATE) between CAST(@initial_pol_eff as DATE) and CAST(@initial_pol_exp as DATE) THEN 1 ELSE 0 END
				,@d_pol_exp_valid = CASE WHEN CAST(@d_pol_exp as DATE) between CAST(@initial_pol_eff as DATE) and CAST(@initial_pol_exp as DATE) THEN 1 ELSE 0 END
	            
		  IF @d_pol_eff_valid = 0
		  BEGIN
			 SET @err_msg = 'APS029: Error - APS - Policy Effective Date NOT in established Policy Effective/Expiration range: Policy Effective Date = ' + CONVERT(VARCHAR(10),@d_pol_eff,121) 
						  + '. Established Policy Effective Date: ' + CONVERT(VARCHAR(10),@initial_pol_eff,121) 
						  + '. Established Policy Expiration Date: ' + CONVERT(VARCHAR(10),@initial_pol_exp,121) 
						  + '. Policy Number: ' + @policy_number 
						  + '. id_sc = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                      
			 EXEC sp_log_errors @num = @pol_num
							   ,@num_cd = 'P'
							   ,@co_name = @co_name
							   ,@cycle_name = @cycle_name
							   ,@proc_name = @proc_name
							   ,@err_msg = @err_msg
							   ,@err_msg_cd = 'E'
							   ,@sql_status = @@ERROR
			 SET @err_warn = NULL
			 SET @err_msg = NULL
			 SET @err_cnt = @err_cnt + 1
		  END
	      
		  IF @d_pol_exp_valid = 0
		  BEGIN
			 SET @err_msg = 'APS030: Error - APS - Policy Expiration Date NOT in established Policy Effective/Expiration range: Policy Expiration Date = ' + CONVERT(VARCHAR(10),@d_pol_eff,121) 
						  + '. Established Policy Effective Date: ' + CONVERT(VARCHAR(10),@initial_pol_eff,121) 
						  + '. Established Policy Expiration Date: ' + CONVERT(VARCHAR(10),@initial_pol_exp,121) 
						  + '. Policy Number: ' + @policy_number 
						  + '. id_sc = ' + CAST(@id_sc as VARCHAR(5)) + '.'
	                      
			 EXEC sp_log_errors @num = @pol_num
							   ,@num_cd = 'P'
							   ,@co_name = @co_name
							   ,@cycle_name = @cycle_name
							   ,@proc_name = @proc_name
							   ,@err_msg = @err_msg
							   ,@err_msg_cd = 'E'
							   ,@sql_status = @@ERROR
			 SET @err_warn = NULL
			 SET @err_msg = NULL
			 SET @err_cnt = @err_cnt + 1
		  END
	         
	   END --IF @new_pol <> 0
   
   END --IF @id_rowcol <> @prev_id_rowcol
  
   --******************************--
   ----------------------------------
   -- Begin XXX Data Inserts --
   ----------------------------------
   --******************************--
   
   INSERT INTO XXX.dbo.XXX
    (lPolicyLiabilityKey
    ,pol_num
    ,trans
    ,vers
    ,d_book
    ,d_pol_eff
    ,d_pol_exp
    ,d_tran_eff
    ,d_tran_exp
    ,d_tran_proc
    ,num_co
    ,pol_num_legacy
    ,rpt_co_name
    ,co_name
    ,underwriter
    ,producer_code
    ,producer_name
    ,aslob
    ,subline
    ,class
    ,cvg_desc
    ,product_cd
    ,product_grp_cd
    ,insd_name
    ,insd_addr
    ,insd_city
    ,insd_state
    ,insd_zip
    ,num_loc
    ,pymt_term
    ,lmt_type
    ,lmt_cvg_s
    ,lmt_pol_s
    ,cvg_attach_point
    ,pol_attach_point
    ,wp_s_pct
    ,reins_retained_pct
    ,fac_reins
    ,reins_special
    ,reins_type
    ,reins_nonoblig_type
    ,reins_nonoblig_pgm_type
    ,reins_nonoblig_ces_rate
    ,data_source
    ,amt_type
    ,amt_desc
    ,amt
    ,ADD_DED_pct
    ,d_updated
    ,d_inserted
    ,id_job
    ,id_bat
    ,f_clsd
    ,cvg_state
    ,product
    ,sub_product)
   VALUES
    (@lPolicyLiabilityKey
    ,@pol_num
    ,@trans
    ,@vers
    ,@d_book
    ,@d_pol_eff
    ,@d_pol_exp
    ,@d_tran_eff
    ,@d_tran_exp
    ,@d_tran_proc
    ,@num_co
    ,@pol_num_legacy
    ,@rpt_co_name
    ,@co_name
    ,@underwriter
    ,@producer_code
    ,@producer_name
    ,@aslob
    ,@subline
    ,@class
    ,@cvg_desc
    ,@product_cd
    ,@product_grp_cd
    ,@insd_name
    ,@insd_addr
    ,@insd_city
    ,@insd_state
    ,@insd_zip
    ,@num_loc
    ,@pymt_term
    ,@lmt_type
    ,@lmt_cvg_s
    ,@lmt_pol_s
    ,@cvg_attach_point
    ,@pol_attach_point
    ,@wp_s_pct
    ,@reins_retained_pct
    ,@fac_reins
    ,@reins_special
    ,@reins_type
    ,@reins_nonoblig_type
    ,@reins_nonoblig_pgm_type
    ,@reins_nonoblig_ces_rate
    ,@data_source
    ,@amt_type
    ,@amt_desc
    ,@amt
    ,@ADD_DED_pct
    ,@d_updated
    ,@d_inserted
    ,@id_job
    ,@id_bat
    ,@f_clsd
    ,@cvg_state
    ,@product
    ,@sub_product)
    
   SET @prev_id_rowcol = @id_rowcol
   
FETCH NEXT
FROM @cXXXXXX
INTO @id_rowcol
	,@id_sc
    ,@account_period
    ,@date_processed
    ,@source_file
    ,@policy_number
    ,@effective_date
    ,@expiration_date
    ,@transaction_code
	,@policy_limit
	,@agency_commission_amount
    ,@transaction_amount
    ,@lPolicyLiabilityKey
    ,@pol_num
    ,@trans
    ,@vers
    ,@d_book
    ,@d_pol_eff
    ,@d_pol_exp
    ,@d_tran_eff
    ,@d_tran_exp
    ,@d_tran_proc
    ,@num_co
    ,@pol_num_legacy
    ,@rpt_co_name
    ,@underwriter
    ,@producer_code
    ,@producer_name
    ,@aslob
    ,@subline
    ,@class
    ,@cvg_desc
    ,@product_cd
    ,@product_grp_cd
    ,@insd_name
    ,@insd_addr
    ,@insd_city
    ,@insd_state
    ,@insd_zip
    ,@num_loc
    ,@pymt_term
    ,@lmt_type
    ,@lmt_cvg_s
    ,@lmt_pol_s
    ,@cvg_attach_point
    ,@pol_attach_point
    ,@wp_s_pct
    ,@reins_retained_pct
    ,@fac_reins
    ,@reins_special
    ,@reins_type
    ,@reins_nonoblig_type
    ,@reins_nonoblig_pgm_type
    ,@reins_nonoblig_ces_rate
    ,@amt_type
    ,@amt_desc
    ,@amt
    ,@ADD_DED_pct
    ,@d_updated
    ,@f_clsd
    ,@cvg_state
    ,@product
    ,@sub_product
       
   IF @@error <> 0 
   BEGIN
      print 'Unable to FETCH data (Repeating Fetch) from cursor @cXXXXXX'
      RETURN -3
   END
      
   -- Reset @new_pol, ensure loops following a new policy are reset to 0
   SET @new_pol = 0
   
END --End while loop

CLOSE @cXXXXXX
DEALLOCATE @cXXXXXX

RETURN 0


