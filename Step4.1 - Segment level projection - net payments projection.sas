options obs=max compress=yes;
%let input_data=rdata.membership_omx_&version._week;
%let dt=today();
%let mode=week;
%let enr_mode=week;
%let interval=260;
%let adj=13;
%let startno=1;
%let grouplevel=productline_cd venuegroup supply_size; *Roll-up level, do not change;

/*****************************************************/
/*Regression data - do not change until update models*/
/* version 1
%let reg_2y=reg.net_write_reg_20170503;
%let reg_5y=reg.net_write_5yr_reg_20170208;
%let reg_5y_cp_dt=reg.proxy_5yr_nwo_cvs_cp_dt_20170208;
%let reg_5y_ic_cs=reg.proxy_5yr_nwo_cvs_ic_cs_20170208;
*/
/* version 2, modified at 2017-07-20*/
%let reg_2y=reg.net_write_reg_20170702;
%let reg_5y=reg.net_write_5yr_reg_20170702;
%let reg_5y_cp_dt=reg.proxy_5yr_nwo_cvs_cp_dt_20170702;
%let reg_5y_ic_cs=reg.proxy_5yr_nwo_cvs_ic_cs_20170702;
/*****************************************************/

/*Use calendar table to format the enroll week/month*/
/*Format the date to week*/
data calendar0;
set biora.tbl_calendar(keep=cal_dt FISCAL_MONTH FISCAL_year WK_NO where=(fiscal_year>=2000));
value=compress(fiscal_year*100+WK_NO);
run;

proc sort nodupkey data=calendar0;
by cal_dt;
run;

/*format enroll week*/
DATA fmt(keep=label fmtname start end);
length label $6.;
set calendar0 end=lastobs;
label=value;
fmtname='fiscal';
start=datepart(cal_dt);
end=datepart(cal_dt);
RUN;

PROC FORMAT CNTLIN=fmt;
RUN;

proc delete data = calendar0;
run;

/*define the cut-off week*/
data dummy;
format this_sun last_sun this_sun_9 this_sun_17 date9.;
 
this_sun = intnx( 'week', &dt., -1, 'beg' );
this_sun_9 = intnx( 'week', &dt., -9, 'beg' ); 
this_sun_17 = intnx( 'week', &dt., -17, 'beg' ); 
this_sun_36 = intnx( 'week', &dt., -36, 'beg' ); 
this_sun_52 = intnx( 'week', &dt., -52, 'beg' ); 
last_sun = intnx( 'week', &dt., -260, 'beg' ); 

call symput('this_sun',compress(put(this_sun,fiscal.)));
call symput('this_sun_9',compress(put(this_sun_9,fiscal.)));
call symput('this_sun_17',compress(put(this_sun_17,fiscal.)));
call symput('this_sun_36',compress(put(this_sun_36,fiscal.)));
call symput('this_sun_52',compress(put(this_sun_52,fiscal.)));
call symput('last_sun',compress(put(last_sun,fiscal.)));
run;

%put &this_sun;
%put &this_sun_9.;
%put &this_sun_17.;
%put &last_sun.;

/*Get sequence number of each week in calendar table*/
data calendar;
set biora.tbl_calendar(keep=cal_dt FISCAL_MONTH FISCAL_year WK_NO where=(fiscal_year>=2000));
enr_week=compress(fiscal_year*100+WK_NO);
run;

proc sort nodupkey data=calendar;
by enr_week;
run;

data calendar;
set calendar;
n=_n_;
run;

data fmt2(keep=label fmtname start end);
length label $6.;
set calendar end=lastobs;
label=n;
fmtname='fiscaln';
start=enr_week * 1;
end=enr_week * 1;
run;

proc format cntlin=fmt2;
run;

/*Calculate the sequence number of current week*/
proc sql noprint;
select n into :max_week
from calendar
where enr_week="&this_sun.";
quit;

/*Data input and roll-up*/
data input_data0;
set &input_data.(rename=(venuegroup=venue));
where ship_continuity_id ne '';

format venuegroup $11.;
if venue = 'Internet' then venuegroup = 'Internet';
else if venue = 'Television' then venuegroup = 'Television';
else venuegroup = 'Other Venue';

week = week + 1;

if sales_amt = . then sales_amt = 0;
if ship_hand_amt = . then ship_hand_amt = 0;
if ret_amt = . then ret_amt = 0;
if ret_ship_hand_amt = . then ret_ship_hand_amt = 0;
if clm_amt = . then clm_amt = 0;
if clm_ship_hand_amt = . then clm_ship_hand_amt = 0;
if adj_amt = . then adj_amt = 0;
if adj_ship_hand_amt = . then adj_ship_hand_amt = 0;
if wo_amt = . then wo_amt = 0;
if wo_ship_hand_amt = . then wo_ship_hand_amt = 0;

if ret_amt + ret_ship_hand_amt ne 0 and ret_amt + ret_ship_hand_amt = clm_amt + clm_ship_hand_amt + adj_amt + adj_ship_hand_amt then do;
    clm_amt = 0;
    clm_ship_hand_amt = 0;
    adj_amt = 0;
    adj_ship_hand_amt = 0;
end;
run;

proc summary data=input_data0 nway noprint missing;
class &grouplevel. kitsizenew enr_week week;
var sales_amt SHIP_HAND_AMT ret_amt ret_ship_hand_amt clm_amt clm_ship_hand_amt adj_amt adj_ship_hand_amt wo_amt wo_ship_hand_amt;
output out=input_data(drop=_type_ _freq_) sum=;
run;

/*Calculate the cohort level #starts*/
proc sort data = input_data0(keep=&grouplevel. kitsizenew enr_week ship_continuity_id) out=acct_data nodupkey;
by &grouplevel. kitsizenew enr_week ship_continuity_id;
run;

data acct_data;
set acct_data;
starts=1;
run;

proc summary data=acct_data nway noprint missing;
class &grouplevel. kitsizenew enr_week;
var starts;
output out=acct_data1(drop=_freq_ _type_) sum=;
run;

proc sort nodupkey data = acct_data1;
by &grouplevel. kitsizenew enr_week;
run;

proc delete data = acct_data;
run;

/*Get regression data & ratio data*/
proc sort nodupkey data = &reg_2y. out=reg;
by model m;
run;

data proxy_reg;
set &reg_5y_cp_dt &reg_5y_ic_cs;
run;
	
proc sort data = proxy_reg nodupkey;
by model;
run;
	
/*Backup actual data*/
data rdata.wo_actual_data_&last_version.;
set rdata.wo_actual_data_all;
run;

proc sort data = rdata.wo_actual_data_all out=actual_data_all nodupkey;
by model enr_week week;
run;

/*Start froecast*/
%macro forecast(output=model sequence,
productline_cd=product,venuegroup=venuegroup,supplysize=supply_size,kitsize=kitsize,kitin=in or not,
minterval=minterval,proxy_flag=proxy flag,proxy_age=proxy age);
/*Get cohort level #starts*/
data acct_data_&output.;
set acct_data1;
where productline_cd in (&productline_cd.) 
and venuegroup in (&venuegroup.)
and supply_size in (&supplysize.)
and kitsizenew &kitin. &kitsize.
and enr_week > "&last_sun."
and enr_week <= "&this_sun_9.";
run;

proc summary data=acct_data_&output. nway noprint missing;
class &grouplevel. enr_week;
var starts;
output out=acct_data1_&output.(drop=_freq_ _type_) sum=;
run;

proc sort nodupkey data = acct_data1_&output.;
by &grouplevel. enr_week;
run;

proc delete data = acct_data_&output.;
run;

/*Get data from input data*/
%if &supplysize. = '30-90' %then %do;
    data fcsales0;
    set input_data;
    where productline_cd in (&productline_cd.) 
    and venuegroup in (&venuegroup.)
    and supply_size in (&supplysize.)
    and kitsizenew &kitin. &kitsize.
    and enr_week > "&last_sun."
    and enr_week <= "&this_sun_9."
    and week <= &minterval.;
    run;
%end;
%else %if &supplysize. = '90-90' %then %do;
    data fcsales0;
    set input_data;
    where productline_cd in (&productline_cd.) 
    and venuegroup in (&venuegroup.)
    and supply_size in (&supplysize.)
    and kitsizenew &kitin. &kitsize.
    and enr_week > "&last_sun."
    and enr_week <= "&this_sun_17."
    and week <= &minterval.;
    run;
%end;

/*Summarize the sales data*/
proc summary data=fcsales0 nway noprint missing;
class &grouplevel. enr_week week;
var sales_amt ship_hand_amt ret_amt ret_ship_hand_amt clm_amt clm_ship_hand_amt adj_amt adj_ship_hand_amt wo_amt wo_ship_hand_amt;
output out=fcsales0a(drop=_type_ rename=(_freq_=shipments)) sum=;
run;

/*The sales data do not have all data, so we need to append standard enroll week & week*/
data enr_week_std;
set rdata.enrweek_std_model;
where model = "&output."
and week <= &interval.;
run;

proc sort data = fcsales0a(keep=&grouplevel. enr_week) out=fcsales0b nodupkey;
by enr_week;
run;

proc sort data = enr_week_std;
by enr_week;
run;

data enr_week_std_1;
merge enr_week_std(in=a)
fcsales0b(in=b);
by enr_week;
if b;
run;

proc sort data = enr_week_std_1 nodupkey;
by &grouplevel. enr_week week;
run;

proc sort data = fcsales0a;
by &grouplevel. enr_week week;
run;

data fcsales0c tmp;
merge fcsales0a(in=a)
enr_week_std_1(in=b);
by &grouplevel. enr_week week;
if b then output fcsales0c;
if a and not b then output tmp;
run; 

proc delete data = tmp;
run;

/*Append #starts information start*/
proc sort data = fcsales0c;
by &grouplevel. enr_week week;
run;

data fcsales;
merge fcsales0c(in=a)
acct_data1_&output.(in=b);
by &grouplevel. enr_week;
if a;
run;

proc delete data = fcsales0 fcsales0a fcsales0b fcsales0c;
run;
/*Append #starts information end*/

proc sort data = fcsales;
by enr_week;
run;

proc sort nodupkey data = calendar;
by enr_week;
run;

data fcsales;
merge fcsales(in=a)
calendar(in=b keep=enr_week n);
by enr_week;
if a;
run;

proc sort data = fcsales;
by &grouplevel. enr_week week;
run;

/*Calculate the cumulative net sales(cumtmp) and net payments(cumsales)*/
data fcsales1(drop=cumtmp startavgsales sales_amt ship_hand_amt ret_amt ret_ship_hand_amt clm_amt clm_ship_hand_amt adj_amt adj_ship_hand_amt wo_amt wo_ship_hand_amt);
format m 3.
model $4.;
set fcsales;
by &grouplevel. enr_week;
retain cumsales 0 startavgsales 0 cumtmp 0;
if first.enr_week then do;
    cumsales = sum(0,sales_amt,ship_hand_amt) - sum(0,ret_amt,ret_ship_hand_amt,clm_amt,clm_ship_hand_amt,adj_amt,adj_ship_hand_amt) - sum(0,wo_amt,wo_ship_hand_amt);
	cumtmp = sum(0,sales_amt,ship_hand_amt) - sum(0,ret_amt,ret_ship_hand_amt,clm_amt,clm_ship_hand_amt,adj_amt,adj_ship_hand_amt);
	
	/*if the cumsales & cumtmp is zero, the inc_rate & np_rate will get an error*/
    if cumsales = 0 then cumsales = 0.01;
	if cumtmp = 0 then cumtmp = 0.01;
	
    avgsales = cumsales/starts;
    startavgsales = avgsales;
    inc_rate = 1;
	np_rate = 1;
end;
else do;
    cumsales = cumsales + sum(0,sales_amt,ship_hand_amt) - sum(0,ret_amt,ret_ship_hand_amt,clm_amt,clm_ship_hand_amt,adj_amt,adj_ship_hand_amt) - sum(0,wo_amt,wo_ship_hand_amt);
    cumtmp = cumtmp + sum(0,sales_amt,ship_hand_amt) - sum(0,ret_amt,ret_ship_hand_amt,clm_amt,clm_ship_hand_amt,adj_amt,adj_ship_hand_amt);
	
	if cumsales = 0 then cumsales = 0.01;
	if cumtmp = 0 then cumtmp = 0.01;

    avgsales = cumsales/starts;
    inc_rate = (avgsales)/startavgsales;
	np_rate = cumsales / cumtmp;
end;

m = week;
model = "&output.";
run;

/*Get the actual data*/
data actual_&version._&output.;
set fcsales1;
where &max_week. - n + 1 >= m;
drop m shipments n;
run;

proc sort data = actual_&version._&output.;
by model enr_week week;
run;

data new_actual_&version._&output.;
merge actual_data_all(in=a keep=model enr_week week)
actual_&version._&output.(in=b);
by model enr_week week;
if not a and b;
run;

proc append base=rdata.wo_actual_data_all data=new_actual_&version._&output. force;
run;
/*Get Actual Data complete*/

/*Append regression data*/
proc sort data = fcsales1;
by m;
run;

data reg_&output.;
format model $4.;
set reg;
where model = "&output.";
run;

proc sort data = reg_&output.;
by m;
run;

data fcsales2;
merge fcsales1(in=a)
reg_&output.(in=b);
by m;
if a;
if intercept = . then intercept = 0;
%do l = 2 %to 104;
    if week&l. = . then week&l. = 0;
%end;
/*We need to adjust the net payments for some cohorts, so we need add the adjust flag for these cohorts*/
if n + m - 1 + 36 >= &max_week. and week <= 104 then np_adj_flag = 'Y';
else np_adj_flag = 'N';
run;

/*Calculate the net payments rate*/
data np_rate_&output.;
set fcsales2;
where enr_week < "&this_sun_52." and week <= 36;
np_adj_flag = 'Y';
run;

proc sort data = np_rate_&output.;
by np_adj_flag model enr_week week;
run;

/*Transpose data*/
proc transpose data = np_rate_&output. out=np_trans_&output. prefix=age;
var np_rate;
by np_adj_flag model enr_week starts;
id week;
run;

/*Roll-up data*/
proc sql;
	create table np_trans_&output._rollup as
	select np_adj_flag,model,(sum(age1 * starts) / sum(starts)) as avg_age1 %do i=2 %to 36; ,(sum(age&i. * starts) / sum(starts)) as avg_age&i. %end;
	from np_trans_&output.
	group by 1,2;
quit;

proc transpose data = np_trans_&output._rollup out=np_trans_&output._rollup1;
var avg_age1 - avg_age36;
by np_adj_flag model;
run;

data np_trans_&output._rollup1;
set np_trans_&output._rollup1;
week = substr(_NAME_,8,3) * 1;
rename col1 = np_adj_rate;
drop _NAME_;
run;
/*Calculate the net payments rate end*/

/*Append the net payments rate*/
proc sort data = np_trans_&output._rollup1;
by np_adj_flag model week;
run;

proc sort data = fcsales2;
by np_adj_flag model week;
run;

data fcsales2;
merge fcsales2(in=a)
np_trans_&output._rollup1(in=b keep=np_adj_flag model week np_adj_rate);
by np_adj_flag model week;
if a;
if np_adj_rate = . then np_adj_rate = 1;
run;

proc sort data = fcsales2;
by &grouplevel. enr_week week;
run;

data fcsales2(drop=startavgsales preavgsales);
set fcsales2;
by &grouplevel. enr_week;
retain startavgsales 0 preavgsales 0;

/*use net payments adjustment rate to calculate the net payments and inc_rate for cohorts enrolled in last 36 weeks*/
if first.enr_week then do;
	avgsales = avgsales * np_adj_rate;
	startavgsales = avgsales;
	preavgsales = avgsales;
end;
else do;
	if n + m - 1 + 36 >= &max_week. and week <= 104 then do;
		avgsales = avgsales * np_adj_rate;
		/*
		If the cumulative average net payments is small than previous avgerage net payments after we did ajustment, 
		we need set the average net payments equal to previous net payments
		*/
		if avgsales < preavgsales then avgsales = preavgsales;
	end;
	preavgsales = avgsales;	
end;
inc_rate = avgsales / startavgsales;
run;

proc delete data = np_rate_&output. np_trans_&output. np_trans_&output._rollup np_trans_&output._rollup1;
run;
/*Append the net payments rate end*/

/*Use regression model and ratio data to start projection*/
proc sort data = fcsales2;
by &grouplevel enr_week week;
run;

data fcsales3_&output.(drop=week2 - week104 inc_rate_2 - inc_rate_104);
set fcsales2;
by &grouplevel enr_week;
retain next_inc_rate 0 startavgsales 0 preavgsales 0 
%do m = 2 %to 104; 
    inc_rate_&m. 0 
%end;;
format fc_flag $8.;

if first.enr_week then do;
    fc_flag = 'ACTUAL';
    next_inc_rate = 0;
    fc_inc_rate = 1;
    %do n = 2 %to 104; 
        inc_rate_&n. = 0; 
    %end;
    startavgsales = avgsales;
    preavgsales = avgsales;
    fc_avgsales = avgsales;
end;
else do;
    if &max_week. - n >= m - &startno. then do;
        fc_flag = 'ACTUAL';
        fc_inc_rate = inc_rate;
		/*use Aonm model to calculate the inc_rate of ageN*/
        %do o = 2 %to 104; 
            if m = &o. then do;
                inc_rate_&o. = fc_inc_rate; 
                next_inc_rate = intercept %do p = 2 %to &o.; + inc_rate_&p. * week&p. %end;;
            end;
        %end;;
        fc_avgsales = avgsales;
        preavgsales = avgsales;
    end;
    else do;
        fc_flag = 'FORECAST';
        fc_inc_rate = next_inc_rate;
		/*use Aonm model to calculate the inc_rate of ageN*/
        %do o = 2 %to 104; 
            if m = &o. then do;
                inc_rate_&o. = fc_inc_rate; 
                next_inc_rate = intercept %do p = 2 %to &o.; + inc_rate_&p. * week&p. %end;;
            end;
        %end;;
     
		fc_avgsales = startavgsales * fc_inc_rate;
        preavgsales = fc_avgsales;
    end;
end;
run;

/*Projection end*/

data fcsales3_&output.;
format model $4.;
set fcsales3_&output.;
model = "&output.";
age_in_weeks = &max_week. - put(enr_week * 1,fiscaln.) * 1 + 1;
run;

data actual_&output.(rename=(fc_avgsales=act_avgsales fc_inc_rate=act_inc_rate)) forcast_&output.;
set fcsales3_&output.(keep=model enr_week fc_inc_rate fc_avgsales n m starts);
where m <= 104;
if &max_week. - n = m - &startno. then output actual_&output.;
else if &max_week. - n = m -&startno. - 1 then output forcast_&output.;
run;

/*output*/
proc transpose data = fcsales3_&output. out=fc_rate_&output. prefix=age;
var fc_inc_rate;
by &grouplevel. enr_week age_in_weeks startavgsales;
id week;
run;

proc sort data = fc_rate_&output.;
by &grouplevel. enr_week;
run;

data fc_rate_&output.;
format model $4.;
merge acct_data1_&output.(in=b keep=&grouplevel. enr_week starts)
fc_rate_&output.(in=a);
by &grouplevel. enr_week;
if a;
model = "&output.";
enr_week_no = age_in_weeks;
if age_in_weeks < 104 then age_in_weeks = 104;
run;

/*2y projection adjustment*/
/*Get adjustment data*/
proc sort data = fc_rate_&output. out=ratetmp(keep=model enr_week starts) nodupkey;
by model enr_week;
run;

proc sort data = rdata.net_write_audit_&last_version._v3(where=(type='% difference')) out=audit0;
by model enr_week;
run;

data audit0;
merge audit0(in=a)
ratetmp(in=b);
by model enr_week;
if a;
if starts >= 50 then u_flag = 1;
else u_flag = 0;
run;

proc sort data = audit0;
by model descending enr_week;
run;

data audit1;
set audit0;
by model;
retain unum 0;
if first.model then do;
	unum = 0;
end;
unum = unum + u_flag;
if u_flag = 1 and unum <= &adj. then output;
run;

proc delete data = ratetmp;
run;

%macro loopaudit;
proc sql;
	create table audit_rollup as
	select model,(sum(age9 * starts) / sum(starts)) as avg9 %do i=10 %to 103; ,(sum(age&i. * starts) / sum(starts)) as avg&i. %end;
	from audit1
	group by 1;
quit;
%mend;

%loopaudit;

/*Transpose*/
proc transpose data = audit_rollup out=audit_rollup_0;
var avg9 - avg103;
by model;
run;

data audit_rollup_0(drop=_NAME_);
set audit_rollup_0;
enr_week_no = substr(_NAME_,4,3) * 1;
rename col1 = validation_error;
run;

/*Use proxy to replace the IC & CS w48*/
data audit_rollup_1a;
set audit_rollup_0;
where model not in ('w42','w43','w44','w45','w48');
run;

data audit_rollup_1b;
set audit_rollup_0(in=a where=(model='w24'))
audit_rollup_0(in=b where=(model='w25'))
audit_rollup_0(in=c where=(model='w24'))
audit_rollup_0(in=d where=(model='w25'))
audit_rollup_0(in=e where=(model='w28'));

if a then model = 'w42';
else if b then model = 'w43';
else if c then model = 'w44';
else if d then model = 'w45';
else if e then model = 'w48';
run;

data audit_rollup_1;
set audit_rollup_1a
audit_rollup_1b;
run;
/**/

proc sort data = audit_rollup_1 nodupkey;
by model enr_week_no;
run;

proc sort data = fc_rate_&output. nodupkey;
by model enr_week_no;
run;

data fc_rate_&output.;
merge fc_rate_&output.(in=a rename=(age104=org_age104))
audit_rollup_1(in=b);
by model enr_week_no;
if a;
if validation_error = . then validation_error = 0;
age104 = org_age104 / (1 + validation_error);
run;

proc delete data = audit audit1 audit_rollup audit_rollup_1;
run;

/*5Y Projection*/
%if &proxy_flag. = 'Y' %then %do;
	proc sort data = fc_rate_&output.;
	by model;
	run;
	
	data fc_rate_2_&output.(drop=week1 - week260);
	merge fc_rate_&output(in=a)
	proxy_reg(in=b keep=model week1 - week260);
	by model;
	if a;
	/*If age in weeks less than 2 year, then use age104 to project age260*/
	if enr_week_no <= 104 then do;
		age260 = age104 * (week260 / week104);
	end;
	/*If age in weeks greater than 2 year, then use the latest age to project age260*/
	else if enr_week_no > 104 then do;
		%do i = 104 %to 259;
			if &proxy_age. = 259 then do;
				if &i. = enr_week_no then do;
					age260 = age&i. * (week260 / week&i.);
				end;
			end;
			else if &proxy_age. < 259 then do;
				if &proxy_age. >= enr_week_no then do;
					if &i. = enr_week_no then do;
						age260 = age&i. * (week260 / week&i.);
					end;
				end;
				else if &proxy_age. < enr_week_no then do;
					age260 = age&proxy_age. * (week260 / week&proxy_age.);
				end;
			end;
		%end;
	end;
	run;
%end;
%else %do;
	proc sort data = fc_rate_&output.;
	by model age_in_weeks;
	run;

	proc sort data = &reg_5y out=reg_5y_&output. nodupkey;
	by model age_in_weeks;
	run;

	data fc_rate_2_&output.(drop=intercept ratio);
	merge fc_rate_&output.(in=a)
	reg_5y_&output.(in=b keep=model age_in_weeks intercept ratio);
	by model age_in_weeks;
	if a;

	%do j=104 %to 259;
		if age_in_weeks = &j. then do;
			age260 = intercept + age&j. * ratio;
		end;
	%end;
	run;
%end;

data fc_sale_&output.;
set fc_rate_2_&output.;
%do i=1 %to 259;
	age&i. = age&i. * startavgsales;
%end;
org_age104 = org_age104 * startavgsales;
age260 = age260 * startavgsales;
run;

proc delete data=fcsales fcsales0 fcsales1 fcsales2 fcsales3_&output. reg_&output. ratio_&output. acct_data1_&output. fc_rate_&output.;
run;

%mend;

/*CD*/
%forecast(output=w1,productline_cd='CD',venuegroup='Internet',supplysize='30-90',kitsize=('2pc','3pc'),kitin=in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w2,productline_cd='CD',venuegroup='Internet',supplysize='30-90',kitsize=('4pc','5pc'),kitin=in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w3,productline_cd='CD',venuegroup='Internet',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w4,productline_cd='CD',venuegroup='Internet',supplysize='90-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w5,productline_cd='CD',venuegroup='Television',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w6,productline_cd='CD',venuegroup='Television',supplysize='90-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w46,productline_cd='CD',venuegroup='Other Venue',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w47,productline_cd='CD',venuegroup='Other Venue',supplysize='90-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);

/*PA*/
%forecast(output=w7,productline_cd='PA',venuegroup='Internet',supplysize='30-90',kitsize=('3pc'),kitin=in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w8,productline_cd='PA',venuegroup='Internet',supplysize='30-90',kitsize=('5pc'),kitin=in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w9,productline_cd='PA',venuegroup='Internet',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w10,productline_cd='PA',venuegroup='Internet',supplysize='90-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w11,productline_cd='PA',venuegroup='Television',supplysize='30-90',kitsize=('3pc'),kitin=in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w12,productline_cd='PA',venuegroup='Television',supplysize='30-90',kitsize=('5pc'),kitin=in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w13,productline_cd='PA',venuegroup='Television',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w14,productline_cd='PA',venuegroup='Television',supplysize='90-90',kitsize=('3pc'),kitin=in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w15,productline_cd='PA',venuegroup='Television',supplysize='90-90',kitsize=('5pc'),kitin=in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w16,productline_cd='PA',venuegroup='Television',supplysize='90-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w17,productline_cd='PA',venuegroup='Other Venue',supplysize='30-90',kitsize=('3pc'),kitin=in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w18,productline_cd='PA',venuegroup='Other Venue',supplysize='30-90',kitsize=('5pc'),kitin=in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w19,productline_cd='PA',venuegroup='Other Venue',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w20,productline_cd='PA',venuegroup='Other Venue',supplysize='90-90',kitsize=('3pc'),kitin=in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w21,productline_cd='PA',venuegroup='Other Venue',supplysize='90-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);

/*MT*/
%forecast(output=w22,productline_cd='MT',venuegroup='Internet',supplysize='30-90',kitsize=('5pc'),kitin=in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w23,productline_cd='MT',venuegroup='Internet',supplysize='30-90',kitsize=('7pc'),kitin=in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w24,productline_cd='MT',venuegroup='Internet',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w25,productline_cd='MT',venuegroup='Internet',supplysize='90-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w26,productline_cd='MT',venuegroup='Television',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w27,productline_cd='MT',venuegroup='Television',supplysize='90-90',kitsize=('7pc'),kitin=in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w28,productline_cd='MT',venuegroup='Television',supplysize='90-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);
%forecast(output=w29,productline_cd='MT',venuegroup='Other Venue',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='N',proxy_age=259);

/*CP*/
%forecast(output=w30,productline_cd='CP',venuegroup='Internet',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='Y',proxy_age=259);
%forecast(output=w31,productline_cd='CP',venuegroup='Internet',supplysize='90-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='Y',proxy_age=259);
%forecast(output=w32,productline_cd='CP',venuegroup='Television',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='Y',proxy_age=259);
%forecast(output=w33,productline_cd='CP',venuegroup='Television',supplysize='90-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='Y',proxy_age=259);

/*DT*/
%forecast(output=w34,productline_cd='DT',venuegroup='Internet',supplysize='30-90',kitsize=('1pc'),kitin=in,minterval=&interval.,proxy_flag='Y',proxy_age=259);
%forecast(output=w35,productline_cd='DT',venuegroup='Internet',supplysize='30-90',kitsize=('3pc'),kitin=in,minterval=&interval.,proxy_flag='Y',proxy_age=259);
%forecast(output=w36,productline_cd='DT',venuegroup='Internet',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='Y',proxy_age=259);
%forecast(output=w37,productline_cd='DT',venuegroup='Internet',supplysize='90-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='Y',proxy_age=240);

/*CS*/
%forecast(output=w38,productline_cd='CS',venuegroup='Internet',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='Y',proxy_age=259);
%forecast(output=w39,productline_cd='CS',venuegroup='Internet',supplysize='90-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='Y',proxy_age=259);
%forecast(output=w40,productline_cd='CS',venuegroup='Television',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='Y',proxy_age=259);
%forecast(output=w41,productline_cd='CS',venuegroup='Television',supplysize='90-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='Y',proxy_age=259);
/*add new segment s84 - CS_INT_30_5, 2017-07-19*/
%forecast(output=w48,productline_cd='CS',venuegroup='Internet',supplysize='30-90',kitsize=('5pc'),kitin=in,minterval=&interval.,proxy_flag='Y',proxy_age=259);

/*IC*/
%forecast(output=w42,productline_cd='IC',venuegroup='Internet',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='Y',proxy_age=259);
%forecast(output=w43,productline_cd='IC',venuegroup='Internet',supplysize='90-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='Y',proxy_age=259);
%forecast(output=w44,productline_cd='IC',venuegroup='Television',supplysize='30-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='Y',proxy_age=259);
%forecast(output=w45,productline_cd='IC',venuegroup='Television',supplysize='90-90',kitsize=('all'),kitin=not in,minterval=&interval.,proxy_flag='Y',proxy_age=259);

data rdata.fc_wo_sale_&version.;
set fc_sale_w1 - fc_sale_w48;
run;

data rdata.fc_wo_rate_&version.;
set fc_rate_2_w1 - fc_rate_2_w48;
run;

data rdata.wo_forcast_&version.;
set forcast_w1 - forcast_w48;
run;

data rdata.wo_actual_&version.;
set actual_w1 - actual_w48;
run;

proc sort data = rdata.wo_forcast_&last_version. out=forcast_&last_version. nodupkey;
by model enr_week;
run;

proc sort data = rdata.wo_actual_&version. out=actual_&version. nodupkey;
by model enr_week;
run;

data rdata.wo_compare_&version.;
merge forcast_&last_version.(in=a)
actual_&version.(in=b);
by model enr_week;
if a and b;
format diff_value 10.2
diff_pct percent8.2;

diff_value = fc_avgsales - act_avgsales;
diff_pct = diff_value / act_avgsales;
run;

data rdata.new_wo_actual_&version.;
set new_actual_&version._w1 - new_actual_&version._w48;
run;

/*Append New data*/
data rdata.fc_wo_sales_all_&last_version.;
set rdata.fc_wo_sales_all;
run;

data rdata.fc_wo_sales_all;
set rdata.fc_wo_sales_all(in=a where=(update_flag ne &version.))
rdata.fc_wo_sale_&version.(in=b drop=_NAME_ rename=(age104=adj_age104 org_age104=age104));
if b then do;
	update_flag = &version.;
	update_date = MDY(substr(left(update_flag),5,2),substr(left(update_flag),7,2),substr(left(update_flag),1,4));
	update_week = put(update_date,fiscal.);
	this_sun = intnx( 'week', update_date, -1, 'beg');
	this_week = put(this_sun,fiscal.);
end;
run;

/*Export process files*/
data calendar1;
set biora.tbl_calendar(keep=cal_dt FISCAL_MONTH FISCAL_year WK_NO where=(fiscal_year>=2000));
format c_dt MMDDYYS10.;
c_dt = datepart(cal_dt);
w_dt = weekday(c_dt);
enr_week=(fiscal_year*100+WK_NO);
run;

proc sort data=calendar1;
by enr_week cal_dt;
run;

data calendar1;
set calendar1;
by enr_week;
if last.enr_week then do;
	if w_dt = 1 then output;
end;
run;

/*format enroll week*/
DATA fmt3(keep=label fmtname start end);
format label MMDDYYS10.;
set calendar1 end=lastobs;
label=c_dt;
fmtname='fiscals';
start=enr_week;
end=enr_week;
RUN;

PROC FORMAT CNTLIN=fmt3;
RUN;

proc delete data = calendar1;
run;

%macro outputnpresult;
/*Sales detail*/
data fc_wo_sale_&version.;
set rdata.fc_wo_sale_&version.;
format modelno 3.
week_ending MMDDYYS10.
%do i=1 %to 260;
age&i. 10.2
%end;
org_age104 10.2
validation_error percent8.3
enr_week1 6.;

enr_week1 = enr_week * 1;
modelno = substr(model,2,3) * 1;
week_ending = put(enr_week1,fiscals.);
run;

/*Rate detail*/
data fc_wo_rate_&version.;
set rdata.fc_wo_rate_&version.;
format modelno 3.
week_ending MMDDYYS10.
%do i=1 %to 260;
age&i. 10.4
%end;
org_age104 10.4
validation_error percent8.3
enr_week1 6.;

enr_week1 = enr_week * 1;
modelno = substr(model,2,3) * 1;
week_ending = put(enr_week1,fiscals.);
run;

/*Validation_Details*/
proc sort data = reg.modellist out=modellist nodupkey;
by model;
run;

proc sort data = rdata.wo_compare_&version. out=wo_compare_&version.;
by model;
run;

data wo_compare_&version.2;
merge wo_compare_&version.(in=a)
modellist(in=b);
by model;
if a;
format modelno 3.
fc_inc_rate 10.4
act_inc_rate 10.4
fc_avgsales 10.2
act_avgsales 10.2
enr_week1 6.
week_ending MMDDYYS10.;
enr_week1 = enr_week * 1;
modelno = substr(model,2,3) * 1;
week_ending = put(enr_week1,fiscals.);
run;

/*Export file*/
proc sql;
	create table fc_wo_sale_&version._output as
	select model,productline_cd,venuegroup,supply_size,enr_week1 as enr_week,week_ending,starts,enr_week_no as age_in_weeks,
	%do i=1 %to 103; age&i., %end;
	age104 as 'age104(adjusted)'n,
	%do i=105 %to 260; age&i., %end;
	org_age104 as 'age104(original)'n,
	validation_error
	from fc_wo_sale_&version.
	where productline_cd not in ('PA','DT')
	order by modelno,enr_week1;
	
	create table fc_wo_rate_&version._output as
	select model,productline_cd,venuegroup,supply_size,enr_week1 as enr_week,week_ending,starts,enr_week_no as age_in_weeks,
	%do i=1 %to 103; age&i., %end;
	age104 as 'age104(adjusted)'n,
	%do i=105 %to 260; age&i., %end;
	org_age104 as 'age104(original)'n,
	validation_error
	from fc_wo_rate_&version.
	where productline_cd not in ('PA','DT')
	order by modelno,enr_week1;
	
	create table wo_compare_&version._output as
	select model,description,enr_week1 as enr_week,week_ending,starts,
	fc_inc_rate,act_inc_rate,fc_avgsales,act_avgsales,diff_value,diff_pct
	from wo_compare_&version.2
	where model not in ('w7','w8','w9','w10','w11','w12','w13','w14','w15','w16','w17','w18','w19','w20','w21','w34','w35','w36','w37')
	order by modelno,enr_week1;
quit;
%mend;

%outputnpresult;

x "del E:\work\br\process\Curve\Report\fc_np_sale_&version..xlsx";
x "del E:\work\br\process\Curve\Report\fc_np_rate_&version..xlsx"; 
x "del E:\work\br\process\Curve\Report\np_validation_details_&version..xlsx";

PROC EXPORT DATA=fc_wo_sale_&version._output
OUTFILE="E:\work\br\process\Curve\Report\fc_np_sale_&version..xlsx"
DBMS=XLSX
REPLACE;
SHEET='fc_np_sale';
RUN;

PROC EXPORT DATA=fc_wo_rate_&version._output
OUTFILE="E:\work\br\process\Curve\Report\fc_np_rate_&version..xlsx"
DBMS=XLSX
REPLACE;
SHEET='fc_np_rate';
RUN;

PROC EXPORT DATA=wo_compare_&version._output
OUTFILE="E:\work\br\process\Curve\Report\np_validation_details_&version..xlsx"
DBMS=XLSX
REPLACE;
SHEET="Validation_Details";
RUN;

proc datasets lib=work KILL;
run;