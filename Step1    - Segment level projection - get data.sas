%let version=20161117; *Version number, need to change every week;
%let last_version=20161109; *Last version number, need to change every week;

/*Stationary configuration, do not change*/
libname biora oracle user="ODS_SAS" password="sas_readonly" path=S_ODSPRD schema="ODS_SAS" or_enable_interrupt=yes db_objects="all";
libname rdata 'E:\work\br\process\Curve\Sales Curve\';
options obs=max compress=yes;

%let reg_version=20161109;
%let dt=today(); *do not change;
%let mode=week; *Version mode, week or month;
%let interval=150; 
%let startno=0; *Start from month/week 0, do not change;
%let grouplevel=productline_cd venuegroup supply_size kitsizenew; *Roll-up level, do not change;
/*Stationary configuration end*/

/*******************************************************************************************************/
/*Step1: Get data from calendar table                                                                  */
/*Step2: Get data                                                                                      */
/*  Step2.1: Get data from biora.vw_sales_attrition table                                              */
/*  Step2.2: Append flexfield2 code to ensure we only get the USA sales data                           */
/*  Step2.3 exclude memberships with missing ship number 1                                             */
/*Step3: append supply size and kitsize from entry order                                               */
/*Step4: append venuegroup                                                                             */
/*Step5: append others amount                                                                          */
/*  Step5.1: append returns                                                                            */
/*  Step5.2: append claims                                                                             */
/*  Step5.3: append misc adj                                                                           */
/*  Step5.4: append writeoff amount                                                                    */
/*  Step5.5: if return amount and claim amount both exists, only keep the return amount                */
/*  Step5.6 summarize data                                                                             */
/*Step6: output file                                                                                   */
/*******************************************************************************************************/

/*Step1: Get data from calendar table*/
data calendar;
set biora.tbl_calendar(keep=cal_dt FISCAL_MONTH FISCAL_year WK_NO where=(fiscal_year>=2000));
value=compress(fiscal_year*100+WK_NO);
valuem=compress(fiscal_year*100+FISCAL_MONTH);
run;

proc sort nodupkey data=calendar;
by cal_dt;
run;

/*format enroll week*/
DATA fmt(keep=label fmtname start end);
length label $6.;
set calendar end=lastobs;
label=value;
fmtname='fiscal';
start=datepart(cal_dt);
end=datepart(cal_dt);
RUN;

PROC FORMAT CNTLIN=fmt;
RUN;

/*format enroll week*/
DATA fmtm(keep=label fmtname start end);
length label $6.;
set calendar end=lastobs;
label=valuem;
fmtname='fiscalm';
start=datepart(cal_dt);
end=datepart(cal_dt);
RUN;

PROC FORMAT CNTLIN=fmtm;
RUN;

/*define the start month and end month*/
data dummy;
startm = year(intnx( 'month', today(), -1*(&interval. + 3), 'beg' )) * 100 + month(intnx( 'month', today(), -1*(&interval. + 3), 'beg' )) ; 
endm = year(intnx( 'month', today(), -1, 'beg' )) * 100 + month(intnx( 'month', today(), -1, 'beg' )); 
call symput('smonth',compress(put(startm,$6.)));
call symput('emonth',compress(put(endm,$6.)));
run;

%put &smonth.;
%put &emonth.;
/*Step1 end*/

/*Step2: Get data*/
/*Step2.1: Get data from biora.vw_sales_attrition table*/
/*In the program, we start from month 1, but in the report, it will be converted into starting from month 0*/
data sales_attrition(keep=sales_detail_id productline_cd orig_venue_cd ship_no customer_account_no sales_amt confirm_start_date enr_week confirm_date 
ship_continuity_id ship_hand_amt item_code week month enr_month);
length orig_venue_cd $2.
week 3.0
month 3.0;

set biora.vw_sales_attrition(keep=sales_detail_id ship_continuity_id customer_account_no ship_no entity dept_cd start_date productline_cd STARTVENUE_CD
sale_amount ship_hand_amt confirm_date record_type start_item_cd
where=(confirm_date>'01jan2008'd and dept_cd not eq "3940" and confirm_start_date <= confirm_date)
rename=(sale_amount = sales_amt start_date = confirm_start_date start_item_cd=item_code));

if record_type="R" then delete;

enr_week = put(datepart(confirm_start_date),fiscal.);
enr_month = put(datepart(confirm_start_date),fiscalm.);;

orig_venue_cd = STARTVENUE_CD;

week = int(datdif(datepart(confirm_start_date),datepart(confirm_date),'act/act')/7) + &startno.;
month = int(datdif(datepart(confirm_start_date),datepart(confirm_date),'act/act')/30.5) + &startno.;

if week < &startno. then week = &startno.;
if month < &startno. then month = &startno.;

membership_id = ship_continuity_id * 1;

if enr_month >= &smonth. and month >= 0 and month <= &interval. and membership_id ne . then output;
run;

proc sort data = sales_attrition;
by sales_detail_id;
run;

/*Step2.2: Append flexfield2 code to ensure we only get the USA sales data*/
data usd_sales;
set biora.tbl_sales_detail(keep=sales_detail_id flexfield2);
where flexfield2 = 'USD';
run;

proc sort nodupkey data = usd_sales;
by sales_detail_id;
run;

data sales;
merge sales_attrition(in=a)
usd_sales(in=b);
by sales_detail_id;
if a and b;
run;

proc delete data = sales_attrition usd_sales;
run;

proc sort data = sales;
by productline_cd ship_continuity_id;
run;

/*Step2.3 exclude memberships with missing ship number 1*/
proc sort nodupkey data = sales(keep=productline_cd ship_continuity_id ship_no where=(ship_no = 1))
    out=entrysales;
by productline_cd ship_continuity_id;
run;

data sales;
merge sales(in=a)
entrysales(in=b keep=productline_cd ship_continuity_id);
by productline_cd ship_continuity_id;
if a and b;
run;
/*Step2 end*/

/*Step3: append supply size and kitsize from entry order*/
proc sort data = sales;
by productline_cd ship_continuity_id ship_no descending sales_amt;
run;

data sales0(keep=productline_cd ship_continuity_id item_code orig_venue_cd enr_week);
set sales;
by productline_cd ship_continuity_id;
if first.ship_continuity_id then output;
run;

proc sort data = sales0;
by item_code;
run;

/*get data from biora.dim_item*/
data dim_item;
format kitsizenew $6.;
set biora.dim_item;
if index(upcase(prod_description),'13PC') > 0 then kitsizenew = '13pc';
else if index(upcase(prod_description),'12PC') > 0 then kitsizenew = '12pc';
else if index(upcase(prod_description),'11PC') > 0 then kitsizenew = '11pc';
else if index(upcase(prod_description),'10PC') > 0 then kitsizenew = '10pc';
else if index(upcase(prod_description),'9PC') > 0 then kitsizenew = '9pc';
else if index(upcase(prod_description),'8PC') > 0 then kitsizenew = '8pc';
else if index(upcase(prod_description),'7PC') > 0 then kitsizenew = '7pc';
else if index(upcase(prod_description),'6PC') > 0 then kitsizenew = '6pc';
else if index(upcase(prod_description),'5PC') > 0 then kitsizenew = '5pc';
else if index(upcase(prod_description),'4PC') > 0 then kitsizenew = '4pc';
else if index(upcase(prod_description),'3PC') > 0 then kitsizenew = '3pc';
else if index(upcase(prod_description),'2PC') > 0 then kitsizenew = '2pc';
else if index(upcase(prod_description),'1PC') > 0 then kitsizenew = '1pc';
else kitsizenew = 'Others';

keep item_code item_supply_size kitsizenew;
run;

proc sort nodupkey data = dim_item;
by item_code;
run;

data sales0;
format supply_size $20.;
merge sales0(in=a)
dim_item(in=b);
by item_code;
if a;
if productline_cd = 'CD' then do;
    if item_supply_size = '14' then supply_size = '14-90';
    else if item_supply_size = '30' then supply_size = '30-90';
    else if item_supply_size = '90' then supply_size = '90-90';
    else supply_size = '';
end;
else if productline_cd = 'CS' then do;
    if item_supply_size = '30' then supply_size = '30-90';
    else if item_supply_size = '90' then supply_size = '90-90';
    else supply_size = '';
end;
else if productline_cd = 'DD' then do;
    if item_supply_size = '30' then supply_size = '30-90';
    else if item_supply_size = '90' then supply_size = '90-90';
    else supply_size = '';
end;
else if productline_cd = 'IC' then do;
    if item_supply_size = '30' then supply_size = '30-90';
    else if item_supply_size = '90' then supply_size = '90-90';
    else supply_size = '';
end;
else if productline_cd = 'MN' then do;
    if item_supply_size = '30' then supply_size = '30-90';
    else if item_supply_size = '90' then supply_size = '90-90';
    else supply_size = '';
end;
else if productline_cd = 'MT' then do;
    if item_supply_size = '30' then supply_size = '30-90';
    else if item_supply_size = '90' then supply_size = '90-90';
    else if item_supply_size = '28' then supply_size = '30-90';
    else supply_size = '';
end;
else if productline_cd = 'CP' then do;
    if item_supply_size = '30' then supply_size = '30-90';
    else if item_supply_size = '60' then supply_size = '60-60';
    else if item_supply_size = '90' then supply_size = '90-90';
    else supply_size = '';
end;
else if productline_cd = 'PD' then do;
    if item_supply_size = '30' then supply_size = '30-90';
    else supply_size = '';
end;
else if productline_cd = 'PA' then do;
    if item_supply_size = '30' then supply_size = '30-90';
    else if item_supply_size = '60' then supply_size = '60-60';
    else if item_supply_size = '90' then supply_size = '90-90';
    else if item_supply_size = '120' then supply_size = '120-90';
    else supply_size = '';
end;
else if productline_cd in ('VY','VE','VI') then do;
    if item_supply_size = '30' then supply_size = '30-90';
    else if item_supply_size = '90' then supply_size = '90-90';
    else supply_size = '';
end;
else if productline_cd = 'DT' then do;
    if item_supply_size = '30' and enr_week <= '201302' then supply_size = '30-60';
    else if item_supply_size = '30' and enr_week > '201302' then supply_size = '30-90';
    else if item_supply_size = '60' then supply_size = '60-60';
    else if item_supply_size = '90' then supply_size = '90-90';
    else supply_size = '';
end;
run;

proc sort data = sales0;
by orig_venue_cd;
run;
/*Step3 end*/

/*Step4: append venuegroup*/
data venue;
set biora.TBL_VENUE;
run;

proc sort nodupkey data=venue;
by orig_venue_cd;
run;

data venue;
set venue;
venuegroup=venuegroup_rpt_alt;
orig_venue_cd=compress(orig_venue_cd||orig_venue_cd);
run;

proc sort data=venue;
by orig_venue_cd;
run;

data sales0;
merge sales0(in=a) 
venue(in=b keep=orig_venue_cd venuegroup);
by orig_venue_cd;
if a;
run;

proc sort nodupkey data = sales0;
by productline_cd ship_continuity_id;
run;

data sales2;
merge sales(in=a)
sales0(in=b keep=productline_cd ship_continuity_id kitsizenew supply_size venuegroup);
by productline_cd ship_continuity_id;
if a;
run;
/*Step4 end*/

/*Step5: append others amount*/
/*Step5.1: append returns*/
data TBL_RETURN;
set biora.TBL_RETURN;
run;

proc sql;
create table returns_hp as
(select a.productline_cd,
        a.sales_detail_id,
		b.ret_qty,
		b.ret_amt as ret_amt length=4,
		b.RET_SHIP_HAND_AMT as RET_SHIP_HAND_AMT length=4,
		b.sale_date as ret_date,
		b.ret_date as ret_date2
from sales2 a,
	 TBL_RETURN b
where 
	b.ffc_cd ne 'OMX' and 
	a.sales_detail_id=b.sales_detail_id and
	datepart(ret_date) <= &dt.
);
quit;

proc sql;
create table returns_omx as
(select a.productline_cd,
        a.sales_detail_id,
		b.ffc_cd as ret_ffc,
		b.ret_qty,
		b.ret_amt as ret_amt length=4,
		b.RET_SHIP_HAND_AMT as RET_SHIP_HAND_AMT length=4,
		b.sale_date as ret_date,
		b.ret_date as ret_date2,
		b.reason
from sales2 a,
	 TBL_RETURN b
where 
	b.ffc_cd = 'OMX' and
	a.sales_detail_id=b.sales_detail_id and
	datepart(ret_date) <= &dt. and
	b.reason not like '%3'
);
quit;

data returns;
set returns_hp returns_omx;
run;

proc delete data=returns_hp returns_omx; run;

/*Step5.2: append claims*/
data TBL_CLAIM;
set biora.TBL_CLAIM;
run;

proc sql;
create table claims_hp as
(select a.productline_cd,
        a.sales_detail_id,
		b.clm_amt,
		b.CLM_SHIP_HAND_AMT,
		b.sale_date as clm_date,
		b.clm_date as clm_date2
from sales2 a,
	 TBL_CLAIM b
where 
	b.ffc_cd ne 'OMX' and
	a.sales_detail_id=b.sales_detail_id and
	datepart(clm_date) <= &dt. 
);
quit;

proc sql;
create table claims_omx as
(select a.productline_cd,
        a.sales_detail_id,
		b.ret_amt as clm_amt length=4,
		b.RET_SHIP_HAND_AMT as CLM_SHIP_HAND_AMT length=4,
		b.sale_date as clm_date,
		b.ret_date as clm_date2,
		b.reason
from sales2 a,
	 TBL_RETURN b
where 	
	b.ffc_cd = 'OMX' and
	a.sales_detail_id=b.sales_detail_id and
	datepart(clm_date) <= &dt. and
	b.reason like '%3' 
);
quit;

data claims;
set claims_hp claims_omx;
run;

proc delete data=TBL_CLAIM TBL_RETURN claims_hp claims_omx; run;

/*Step5.3: append misc adj*/
data TBL_MISCADJ;
set biora.TBL_MISCADJ;
run;

proc sql;
create table miscadj as
(select a.productline_cd,
        a.sales_detail_id,
		((b.eds_tran_cd="600")*adj_amt-(b.eds_tran_cd="620")*adj_amt) as adj_amt length=4,
		((b.eds_tran_cd="600")*adj_ship_hand_amt-(b.eds_tran_cd="620")*adj_ship_hand_amt) as adj_ship_hand_amt length=4,
		b.sale_date as adj_date,
		b.adj_date as adj_date2
from sales2 a,
	 TBL_MISCADJ b
where a.sales_detail_id=b.sales_detail_id and
	  datepart(adj_date) <= &dt.
);
quit;

proc delete data=TBL_MISCADJ; run;

/*Step5.4: append writeoff amount*/
data TBL_WRITEOFF;
set biora.TBL_WRITEOFF;
run;

proc sql;
create table writeoff as
(select a.productline_cd,
        a.sales_detail_id,
		b.wo_amt as wo_amt length=4,
		b.WO_SHIP_HAND_AMT as WO_SHIP_HAND_AMT length=4,
		b.sale_date as wo_date,
		b.wo_date as wo_date2
from sales2 a,
	 TBL_WRITEOFF b
where a.sales_detail_id=b.sales_detail_id and
	  datepart(wo_date) <= &dt.
);
quit;

proc delete data=TBL_WRITEOFF; run;

/*Step5.5: if return amount and claim amount both exists, only keep the return amount*/
proc summary data=returns nway noprint missing;
class sales_detail_id;
var ret_amt ret_ship_hand_amt;
output out=returns1(drop=_type_ rename=(_freq_=shipments)) sum=;
run;

proc sort nodupkey data = returns1;
by sales_detail_id;
run;

proc summary data=claims nway noprint missing;
class sales_detail_id;
var clm_amt clm_ship_hand_amt;
output out=claims1(drop=_type_ rename=(_freq_=shipments)) sum=;
run;

proc sort nodupkey data = claims1;
by sales_detail_id;
run;

proc summary data=miscadj nway noprint missing;
class sales_detail_id;
var adj_amt adj_ship_hand_amt;
output out=miscadj1(drop=_type_ rename=(_freq_=shipments)) sum=;
run;

proc sort nodupkey data = miscadj1;
by sales_detail_id;
run;

proc summary data=writeoff nway noprint missing;
class sales_detail_id;
var wo_amt wo_ship_hand_amt;
output out=writeoff1(drop=_type_ rename=(_freq_=shipments)) sum=;
run;

proc sort nodupkey data = writeoff1;
by sales_detail_id;
run;

proc sort data = sales2;
by sales_detail_id;
run;

/*Step5.6 summarize data*/
data sales3;
merge sales2(in=a)
returns1(in=b keep=sales_detail_id ret_amt ret_ship_hand_amt)
claims1(in=c keep=sales_detail_id clm_amt clm_ship_hand_amt)
miscadj1(in=d keep=sales_detail_id adj_amt adj_ship_hand_amt)
writeoff1(in=e keep=sales_detail_id wo_amt wo_ship_hand_amt);
by sales_detail_id;
if a;
if ret_amt = . then ret_amt = 0;
if ret_ship_hand_amt = . then ret_ship_hand_amt = 0;
if clm_amt = . then clm_amt = 0;
if clm_ship_hand_amt = . then clm_ship_hand_amt = 0;
if adj_amt = . then adj_amt = 0;
if adj_ship_hand_amt = . then adj_ship_hand_amt = 0;
if wo_amt = . then wo_amt = 0;
if wo_ship_hand_amt = . then wo_ship_hand_amt = 0;
run;
/*Step5 end*/

/*Step6: output file*/
/*Summarize the sales data*/
proc summary data=sales3 nway noprint missing;
class &grouplevel. enr_week &mode.;
var sales_amt SHIP_HAND_AMT ret_amt ret_ship_hand_amt clm_amt clm_ship_hand_amt adj_amt adj_ship_hand_amt wo_amt wo_ship_hand_amt;
output out=salesum(drop=_type_ rename=(_freq_=shipments)) sum=;
run;

proc sort data=sales3;
by productline_cd ship_continuity_id ship_no descending sales_amt;
run;

data saleaccts(keep=productline_cd ship_continuity_id supply_size kitsizenew enr_week confirm_start_date VENUEGROUP);
set sales3;
by productline_cd ship_continuity_id;
if first.ship_continuity_id then output;
run;

data accts;
length starts 3.0;
set saleaccts(in=b);
starts=1;
run;

proc summary data=accts nway noprint missing;
class &grouplevel. enr_week;
var starts;
output out=acctsum(drop=_type_ _freq_) sum=;
run;

data rdata.accts_omx_&version._&mode.;
set acctsum;
run;

data rdata.sales_omx_&version._&mode.;
set salesum;
run;

proc delete data = acctsum salesum;
run;

/*Output rollup data*/
/*Output the base condition group*/

proc sort nodupkey data = rdata.sales_omx_&version._&mode. out=ovar(keep=&grouplevel. enr_week);
by &grouplevel. enr_week;
run;

proc sql noprint;
select max(&mode.) into :week_interval
from rdata.sales_omx_&version._&mode.;
quit;

%macro optionvar;
    %do i=&startno. %to %eval(&startno.+&week_interval.-1);
        data optionvar&i.;
        set ovar;
        &mode. = &i.;
        run;

        %if &i.=&startno. %then %do;
            data optionvar;
            set optionvar&i.;
            run;
        %end;
        %else %do;
            proc append base=optionvar data=optionvar&i. force;
            run;
        %end;
        
        proc delete data = optionvar&i.;
        run;
    %end;

    proc sort nodupkey data = optionvar;
    by &grouplevel. enr_week &mode.;
    run;

    proc delete data = ovar;
    run;

%mend;

%optionvar;

/*Report: Sales Curve*/
proc summary data=rdata.sales_omx_&version._&mode. nway noprint missing;
class &grouplevel. enr_week &mode.;
var sales_amt SHIP_HAND_AMT ret_amt ret_ship_hand_amt clm_amt clm_ship_hand_amt adj_amt adj_ship_hand_amt wo_amt wo_ship_hand_amt;
output out=gross_sales(drop=_type_ _freq_) sum=;
run;

proc summary data=rdata.accts_omx_&version._&mode. nway noprint missing;
class &grouplevel. enr_week ;
var starts;
output out=account_counts(drop=_freq_ _type_) sum=;
run;

data gross_sales2;
merge optionvar(in=a)
gross_sales(in=b);
by &grouplevel. enr_week &mode.;
if a;
run;

data rdata.final_omx_&version._&mode.;
merge gross_sales2(in=a)
account_counts(in=b);
by &grouplevel. enr_week;
if a;
run;

/*Roll-up by membership level*/
proc summary data=sales3 nway noprint missing;
class ship_continuity_id &grouplevel. enr_week &mode.;
var sales_amt SHIP_HAND_AMT ret_amt ret_ship_hand_amt clm_amt clm_ship_hand_amt adj_amt adj_ship_hand_amt wo_amt wo_ship_hand_amt;
output out=membership_rollup(drop=_type_ _freq_) sum=;
run;

proc sort data = membership_rollup;
by &grouplevel. enr_week &mode.;
run;

data rdata.membership_omx_&version._&mode.;
merge optionvar(in=a)
membership_rollup(in=b);
by &grouplevel. enr_week &mode.;
if a;
run;

data rdata.org_membership_omx_&version._&mode.;
set membership_rollup;
run;

proc datasets lib=work KILL;
run;