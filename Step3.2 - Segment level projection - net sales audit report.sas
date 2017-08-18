libname biora oracle user="ODS_SAS" password="sas_readonly" path=p_ODSPRD schema="ODS_SAS" or_enable_interrupt=yes db_objects="all";

options compress=yes obs=max;

/*Get others data from calendar*/
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

DATA fmt2(keep=label fmtname start end);
length label $6.;
set calendar end=lastobs;
label=n;
fmtname='fiscaln';
start=enr_week * 1;
end=enr_week * 1;
RUN;

PROC FORMAT CNTLIN=fmt2;
RUN;

/*filter out data needed*/
data act;
set rdata.net_actual_data_all;
where week = 104;
keep model productline_cd venuegroup supply_size enr_week;
run;

proc sort data = act nodupkey;
by model productline_cd venuegroup supply_size enr_week;
run;

proc sort data = rdata.fc_net_sales_all out=fc nodupkey;
by model enr_week update_date;
run;
 
data fc;
set fc;
keep model productline_cd venuegroup supply_size enr_week;
run;

proc sort data = fc nodupkey;
by model productline_cd venuegroup supply_size enr_week;
run;

data filterdata;
merge act(in=a keep=model productline_cd venuegroup supply_size enr_week)
fc(in=b keep=model productline_cd venuegroup supply_size enr_week);
by model productline_cd venuegroup supply_size enr_week;
if a and b;
run;

proc delete data = act fc;
run;

proc sort data = rdata.net_actual_data_all out=actual_data_2yr;
by model productline_cd venuegroup supply_size enr_week;
run;

data actual_data_2yr_9 actual_data_2yr_17;
merge actual_data_2yr(in=a)
filterdata(in=b);
by model productline_cd venuegroup supply_size enr_week;
if a and b;
if supply_size = '30-90' then output actual_data_2yr_9;
else if supply_size = '90-90' then output actual_data_2yr_17;
run;

proc sort nodupkey data = reg.net_sales_reg_20170702(keep=model m intercept week2 - week104 rename=(m=week)) out=reg;
by model week;
run;

%macro loopsales;

%do i=9 %to 103;
	data actual_data_2yr_9a_&i.;
	set actual_data_2yr_9;
	run;
	
	proc sort data = actual_data_2yr_9a_&i.;
	by model week;
	run;
	
	data actual_data_2yr_9a_&i.;
	merge actual_data_2yr_9a_&i.(in=a)
	reg(in=b);
	by model week;
	if a;
	if intercept = . then intercept = 0;
	%do j = 2 %to 104;
		if week&j. = . then week&j. = 0;
	%end;
	run;

	proc sort data = actual_data_2yr_9a_&i.;
	by model enr_week week;
	run;

	data actual_data_2yr_9b_&i.;
	set actual_data_2yr_9a_&i.;
	by model enr_week;
	retain next_inc_rate 0 startavgsales 0 preavgsales 0 
	%do k = 2 %to 104; 
		inc_rate_&k. 0 
	%end;;
	
	format fc_flag $8.;

	if first.enr_week then do;
		fc_flag = 'ACTUAL';
		next_inc_rate = 0;
		fc_inc_rate = 1;
		%do l = 2 %to 104; 
			inc_rate_&l. = 0; 
		%end;
		startavgsales = avgsales;
		preavgsales = avgsales;
		fc_avgsales = avgsales;
	end;
	else do;
		if week <= &i. then do;
			fc_flag = 'ACTUAL';
			fc_inc_rate = inc_rate;
			%do m = 2 %to &i.; 
				if week = &m. then do;
					inc_rate_&m. = fc_inc_rate; 
					next_inc_rate = intercept %do p = 2 %to &m.; + inc_rate_&p. * week&p. %end;;
				end;
			%end;;
			fc_avgsales = avgsales;
			preavgsales = avgsales;
		end;
		else do;
			fc_flag = 'FORECAST';
			fc_inc_rate = next_inc_rate;
			%do n = 2 %to 104; 
				if week = &n. then do;
					inc_rate_&n. = fc_inc_rate; 
					next_inc_rate = intercept %do o = 2 %to &n.; + inc_rate_&o. * week&o. %end;;
				end;
			%end;;
			fc_avgsales = startavgsales * fc_inc_rate;
			preavgsales = fc_avgsales;
		end;
	end;
	update_age = &i.;
	run;
	
	data actual_data_2yr_9c_&i.;
	set actual_data_2yr_9b_&i.;
	where week = 104;
	run;
	
	proc delete data = actual_data_2yr_9a_&i. actual_data_2yr_9b_&i.;
	run;
%end;

%do i=17 %to 103;
	data actual_data_2yr_17a_&i.;
	set actual_data_2yr_17;
	run;
	
	proc sort data = actual_data_2yr_17a_&i.;
	by model week;
	run;
	
	data actual_data_2yr_17a_&i.;
	merge actual_data_2yr_17a_&i.(in=a)
	reg(in=b);
	by model week;
	if a;
	if intercept = . then intercept = 0;
	%do j = 2 %to 104;
		if week&j. = . then week&j. = 0;
	%end;
	run;

	proc sort data = actual_data_2yr_17a_&i.;
	by model enr_week week;
	run;

	data actual_data_2yr_17b_&i.;
	set actual_data_2yr_17a_&i.;
	by model enr_week;
	retain next_inc_rate 0 startavgsales 0 preavgsales 0 
	%do k = 2 %to 104; 
		inc_rate_&k. 0 
	%end;;
	
	format fc_flag $8.;

	if first.enr_week then do;
		fc_flag = 'ACTUAL';
		next_inc_rate = 0;
		fc_inc_rate = 1;
		%do l = 2 %to 104; 
			inc_rate_&l. = 0; 
		%end;
		startavgsales = avgsales;
		preavgsales = avgsales;
		fc_avgsales = avgsales;
	end;
	else do;
		if week <= &i. then do;
			fc_flag = 'ACTUAL';
			fc_inc_rate = inc_rate;
			%do m = 2 %to &i.; 
				if week = &m. then do;
					inc_rate_&m. = fc_inc_rate; 
					next_inc_rate = intercept %do p = 2 %to &m.; + inc_rate_&p. * week&p. %end;;
				end;
			%end;;
			fc_avgsales = avgsales;
			preavgsales = avgsales;
		end;
		else do;
			fc_flag = 'FORECAST';
			fc_inc_rate = next_inc_rate;
			%do n = 2 %to 104; 
				if week = &n. then do;
					inc_rate_&n. = fc_inc_rate; 
					next_inc_rate = intercept %do o = 2 %to &n.; + inc_rate_&o. * week&o. %end;;
				end;
			%end;;
			fc_avgsales = startavgsales * fc_inc_rate;
			preavgsales = fc_avgsales;
		end;
	end;
	update_age = &i.;
	run;

	data actual_data_2yr_17c_&i.;
	set actual_data_2yr_17b_&i.;
	where week = 104;
	run;
	
	proc delete data = actual_data_2yr_17a_&i. actual_data_2yr_17b_&i.;
	run;
%end;

%mend loopsales;

%loopsales;

data fc_actual_data_2yr(drop=intercept week2 - week104 inc_rate_2 - inc_rate_104 next_inc_rate startavgsales preavgsales fc_flag);
set actual_data_2yr_9c_9 - actual_data_2yr_9c_103
actual_data_2yr_17c_17 - actual_data_2yr_17c_103;
rename fc_avgsales = age104;
run;

proc datasets lib=work;
delete actual_data_2yr_9c_9 - actual_data_2yr_9c_103
actual_data_2yr_17c_17 - actual_data_2yr_17c_103;
run;

data fc_actual_data_2yr;
set fc_actual_data_2yr;
format update_flag 8.
update_date date9.
update_week $10.
this_week $10.;

update_flag = .;
update_date = .;
update_week = '';
this_week = '';

keep model productline_cd venuegroup supply_size enr_week age104 update_flag update_date update_week this_week update_age;

if update_age < 104 then output;
run;

/*Get data from process*/
proc sort data = rdata.fc_net_sales_all out=fc_data_2yr nodupkey;
by model enr_week update_date;
run;
 
data fc_data_2yr;
set fc_data_2yr;
update_age = (put(update_week * 1,fiscaln.) * 1) - (put(enr_week * 1,fiscaln.) * 1);
keep model productline_cd venuegroup supply_size enr_week age104 update_flag update_date update_week this_week update_age;
if (supply_size = '30-90' and 9 <= update_age < 104) or (supply_size = '90-90' and 17 <= update_age < 104) then output;
run;

proc sort data = fc_actual_data_2yr nodupkey;
by model enr_week update_age;
run;

proc sort data = fc_data_2yr(keep=model enr_week) out=fc1 nodupkey;
by model enr_week;
run;

data fc_actual_data_2yr_1;
merge fc_actual_data_2yr(in=a)
fc1(in=b);
by model enr_week;
if a and b;
run;

proc sort data = fc_actual_data_2yr_1 nodupkey;
by model enr_week update_age;
run;

proc sort data = fc_data_2yr(keep=model enr_week update_age) out=fc2 nodupkey;
by model enr_week update_age;
run;

data fc_actual_data_2yr_2;
merge fc_actual_data_2yr_1(in=a)
fc2(in=b);
by model enr_week update_age;
if a and not b;
run;

data fc_data_2yr_all;
set fc_data_2yr(in=a)
fc_actual_data_2yr_2(in=b);
format flag $20.;
if a then flag = 'Process';
else if b then flag = 'Forcast Actual';
run;

proc sort data = fc_data_2yr_all;
by model productline_cd venuegroup supply_size enr_week update_age;
run;

data actual_data_2yr;
set rdata.net_actual_data_all;
where week = 104;
keep model productline_cd venuegroup supply_size enr_week week avgsales;
run;

proc sort data = actual_data_2yr;
by model productline_cd venuegroup supply_size enr_week;
run;

data combine_data;
merge actual_data_2yr(in=a)
fc_data_2yr_all(in=b);
by model productline_cd venuegroup supply_size enr_week;
if a and b;
format avgsales 10.2
age104 10.2
diff_value 10.2
diff_pct 10.8;

diff_value = age104 - avgsales;
diff_pct = diff_value / avgsales;

rename avgsales = 'Actual 2y'n
age104 = 'Projected 2y'n
diff_pct = '% difference'n;

if update_age < 104 then output;
run;

proc sort data = combine_data;
by model productline_cd venuegroup supply_size descending enr_week update_age;
run;

proc transpose data = combine_data out=combine_data1 prefix=age;
var 'Actual 2y'n 'Projected 2y'n '% difference'n;
by model productline_cd venuegroup supply_size descending enr_week;
id update_age;
run;

data combine_data2;
set combine_data1;
nnobs = substr(model,2,3) * 1;
run;

proc sort data = combine_data2 out=rdata.net_sales_audit_&version._v3(drop=nnobs rename=(_NAME_ = type));
by nnobs productline_cd venuegroup supply_size enr_week;
run;

proc delete data = combine_data combine_data1 combine_data2 calendar fmt2 actual_data_2yr fc_data_2yr;
run;

data net_sales_audit_&version._v4;
set rdata.net_sales_audit_&version._v3;
where type = '% difference';
run;

proc sort data = net_sales_audit_&version._v4;
by model enr_week;
run;

/*Get starts information*/
proc sort data = rdata.fc_net_sale_&version. out=fc_starts(keep=model enr_week starts) nodupkey;
by model enr_week;
run;

data net_sales_audit_&version._v4;
merge net_sales_audit_&version._v4(in=a)
fc_starts(in=b);
by model enr_week;
if a;
run;

%macro loopavg;
proc sql;
	create table net_sales_audit_&version._v4a as
	select model %do i=9 %to 103; ,(sum(starts * age&i.) / sum(starts)) as age&i %end;
	from net_sales_audit_&version._v4
	group by 1;
quit;
%mend;

%loopavg;

data net_sales_audit_&version._v4b;
set net_sales_audit_&version._v4a;
nnobs = substr(model,2,3) * 1;
run;

proc sort data = net_sales_audit_&version._v4b out=rdata.net_sales_audit_avg_&version.(drop=nnobs);
by nnobs;
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

%macro outputnsaudit;

proc sort data = reg.modellist out=modellist(keep=model Description) nodupkey;
by model;
run;

/*Audit detail*/
proc sort data = rdata.net_sales_audit_&version._v3 out=net_sales_audit_&version._v3;
by model;
run;

data net_sales_audit_&version._v4;
merge net_sales_audit_&version._v3(in=a)
modellist(in=b);
by model;
if a;
format modelno 3.
enr_week1 6.
week_ending MMDDYYS10.;
enr_week1 = enr_week * 1;
modelno = substr(model,2,3) * 1;
week_ending = put(enr_week1,fiscals.);
if type = 'Actual 2y' then order_sequence = 1;
else if type = 'Projected 2y' then order_sequence = 2;
else if type = '% difference' then order_sequence = 3;
run;

/*Audit summary*/
proc sort data = rdata.net_sales_audit_avg_&version. out=net_sales_audit_avg_&version.;
by model;
run;

data net_sales_audit_avg_&version.2;
merge net_sales_audit_avg_&version.(in=a)
modellist(in=b);
by model;
if a;
format modelno 3.;
modelno = substr(model,2,3) * 1;
run;

/*Export file*/
proc sql;
	create table net_sales_audit_&version._output as
	select model,description,enr_week1 as enr_week,week_ending,type 
	%do i=9 %to 103; ,age&i. %end;
	from net_sales_audit_&version._v4
	where model not in ('s17','s18','s19','s20','s21','s22','s23','s24','s25','s26','s27','s28','s29','s30','s31','s32','s33','s34','s35','s59','s60','s61','s62')
	order by modelno,enr_week1,order_sequence;
	
	create table ns_audit_avg_&version._output as
	select model,description
	%do i=9 %to 103; ,age&i. %end;
	from net_sales_audit_avg_&version.2
	where model not in ('s17','s18','s19','s20','s21','s22','s23','s24','s25','s26','s27','s28','s29','s30','s31','s32','s33','s34','s35','s59','s60','s61','s62')
	order by modelno;
quit;
%mend;

%outputnsaudit;

x "del E:\work\br\process\Curve\Report\ns_audit_&version..xlsx";
x "del E:\work\br\process\Curve\Report\ns_audit_avg_&version..xlsx"; 

PROC EXPORT DATA=net_sales_audit_&version._output
OUTFILE="E:\work\br\process\Curve\Report\ns_audit_&version..xlsx"
DBMS=XLSX
REPLACE;
SHEET='ns_audit';
RUN;

PROC EXPORT DATA=ns_audit_avg_&version._output
OUTFILE="E:\work\br\process\Curve\Report\ns_audit_avg_&version..xlsx"
DBMS=XLSX
REPLACE;
SHEET='ns_audit_avg';
RUN;

proc datasets lib=work KILL;
run;