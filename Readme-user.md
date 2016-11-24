00_ABOUT:
-----------

**"Подготовка данных для кластеризации сайтзон для байесовского биддера"** 

01_START_GEEK_DAY:
-----------

**Запуск всех скриптов.** 

01_START_GEEK_DAY.bbid1:
-----------

**Запуск всех скриптов.** 

01_START_GEEK_DAY.dm12:
-----------

**Запуск всех скриптов.** 

01_START_GEEK_HOURS:
-----------

**Запуск всех скриптов.** 

01_START_GEEK_MONDAY:
-----------

**Запуск всех скриптов.** 

01_START_GEEK_MONDAY.bbid1:
-----------

**Запуск всех скриптов.** 

01_START_GEEK_MONDAY.dm12:
-----------

**Запуск всех скриптов.** 

01_STOP_ALL:
-----------

**Остановка всех скриптов.** 
Создается файл 01_STOP_ALL.stop, который перед запуском 01_START_ALL нужно удалить. 

02_target_status_one_line:
-----------

**** 

10_google_tn0:
-----------

**сбор из hl typenum==0 (показы) , sid for google, ssp** 

	 отбрасываются нулевые sz,uid 
	 OUT: 
	  sz,ad,exposureprice,winprice 
	  сортировка по sz, ad. 

10_google_tn1:
-----------

**сбор из hl typenum==1 (клики) , sid for google,ssp** 

	 отбрасываются нулевые sz, uid. 
	 поля: sz,ad 
	 сортировка: по sz,ad 

10_google_tn3:
-----------

**сбор из hl typenum==3 (bids), sids for google, ssp** 

	 отбрасываются нулевые sz,uid. 
	 поля: uid, second, sz, pz, bt, exposureprice, ad, ref 
	 "ref" сплитится на два поля: "dom" и "path" 
	 сортировка: uid, second. 
	 out: 
	  1      2     3   4     5        6           7    8      9     10 
	 uid, second, sz,  pz,  bt,  exposureprice,  ad, expid,  dom,  path 

10_HOURS_BY_HOURS:
-----------

**history_log -------( jobscript )--------> ../RESULT/10/${day}/${job}/${typenum}/yyyy-mm-ddThh.gz** 

	 Сбор часовых данных из hl. 

Параметры: $job и $typenum. 

10_mail_tn0:
-----------

**сбор из hl typenum==0 (показы) , sid for mail** 

	 отбрасываются нулевые sz,uid 
	 OUT: 
	  sz,ad,exposureprice,winprice 
	  сортировка по sz, ad. 

10_mail_tn1:
-----------

**сбор из hl typenum==1 (клики) , sid for mail** 

	 отбрасываются нулевые sz, uid. 
	 поля: sz,ad 
	 сортировка: по sz,ad 

10_mail_tn3:
-----------

**сбор из hl typenum==3 (bids), sids for mail** 

	 отбрасываются нулевые sz,uid. 
	 поля: uid, second, sz, pz, bt, exposureprice, ad, ref 
	 "ref" сплитится на два поля: "dom" и "path" 
	 сортировка: uid, second. 
	 out: 
	  1      2     3   4     5        6           7    8      9     10 
	 uid, second, sz,  pz,  bt,  exposureprice,  ad, expid,  dom,  path 

10_net_tn0:
-----------

**сбор из hl typenum==0 (показы) , network==60** 

	 OUT 
	 uid - для сессий 
	 sec - для сессий 
	 sid - *новое для net 
	 sz 
	 pz - для комбинаций pzbt 
	 bt - для комбинаций pzbt 
	 exppr - для: среднего отношения к winprice, для доли бидов с положит ставкой( если не исп. ad для этого ) ( для выигранных бидов - сетевых просмотров), 
	 secondprice - для: среднего winprice, среднего отношения expprice к winprice ( для выигранных бидов - сетевых просмотров) 
	 ad - для детализации по клиенту, 
	 network - (нужно ли фильтровать 43|44|45|60|0 ?) network=43|44|45|60|0 - показ сетевой (в какой сети) или нет 
	 dom - для bids, sessions, urls 
	 path 

10_net_tn1:
-----------

**сбор из hl typenum==1 (клики) for net** 

10_ssp_tn1:
-----------

**> OUT: sid sz ad** 

10_ssp_tn3:
-----------

**>       1    2     3  4  5  6    7    8  9     10  11  12          13** 

	 OUT: uid second sid sz pz bt exppr ad expid dom path id(custom) wm(custom) 

12_MERGE:
-----------

**Часы (YYYY-MM-HH.gz) ---------( через $posfilter )-----> $job/$tn/total.gz (в сутки)** 
параметр: 
job=${1:? Job! } 
параметр: 
typenum=${2:? Typenum! } 
days: 
Имя скрипта $postfilter зависит от $job и $typenum: 
suffix="" 
prefilter определяет скрипт, который может обработать отдельные потоки файлов до merge: 
prefilter="./12_pref_${job}_tn${typenum}${suffix}" 
не обрабатываются часовые данные, где рядом уже есть total.gz 
не обрабатываются часовые данные с размером меньше или равно 20 байт 

12_postf_google_tn0:
-----------

**Обработка смерженного (часы в сутки) потока для google, tn0.** 

	 IN: 
	  1   2        3          4 
	 sz  ad  exposureprice winprice 
	 OUT: 
	  1   2     3            4                 5 
	 sz  ad  cnt_exp  sum_exposureprice  sum_winprice 
	 
	 | field | description | 
	 | ----- | ----------- | 
	 | sz    | 
	 | ad    | 
	 | cnt_exp | количество показов | 
	 | sum_exposureprice | сумма exposureprice | 
	 | sum_winprice | сумма winprice | 
	 | sum_expprice/winprice | сумма expprice/winprice | 

12_postf_google_tn0_url:
-----------

**Обработка смерженного (часы в сутки) потока для google, tn0, url.** 

	 IN: 
	  1   2      3         4 
	 dom path  expprice winprice 
	 OUT: 
	  1   2     3          4          5 
	 dom path cnt_exp  sum_exppr sum_winprice 
	 | field        | description | 
	 | -----        | ----------- | 
	 | dom          | 
	 | path         | 
	 | cnt_exp      | количество показов | 
	 | sum_exppr    | сумма expprice | # <--- это поле предполагается не использовать, будем брать exppr из tn3 
	 | sum_winprice | сумма winprice | 

12_postf_google_tn1:
-----------

**Обработчик смерженного (часы в сутки) потока для google tn1.** 

	 IN 
	  1   2 
	 sz  ad 
	OUT: 
	  1  2      3 
	 sz ad  cnt_clicks 
	 | field | description | 
	 | ----- | ----------- | 
	 | sz    | 
	 | ad    | 
	 | cnt_clicks | количество кликов  | 

12_postf_google_tn3:
-----------

**Обработчик смерженного (часы в сутки) потока для google tn3.** 

	 Добавляем колонку про сессии: СЕКУНДЫ начала сессии КУКИ на ДОМЕНЕ, 
	 и колонку "признак засчитываемого просмотра страницы" - незасчитываются повторные просмотры одной и тойже страницы с интервалом менее 10 сук. 
	 На входе поток за сутки (т.е период начало и конец которого можно считать разрывом сессии), лучше с 04-00 по 04-00. 
	 отсортрованный по КУКЕ и ВРЕМЕНИ. 
	 OUT: 
	 
	 | field | description | 
	 | ----- | ----------- | 
	 |1 uid   | 
	 |2 sec   | 
	 |3 sz    | 
	 |4 pz    | 
	 |5 bt    | 
	 |6 exposure_price |изза ad не используется| 
	 |7 ad      | 

 |8 expid 

	 |9 domain  | 
	 |10 path    | 
	 |11 sestart | секунды начала сессии   | 
	 |12 isview  | просмотр засчитан (1/0) | 

12_postf_google_tn3_url:
-----------

**> IN:** 

	  1   2        3         4             5       6          7 
	 dom path  cnt_bids  sum_expprice3  cnt_exp sum_winpr sum_exppr0 

12_postf_mail_tn0:
-----------

**Обработка смерженного (часы в сутки) потока для google, tn0.** 

	 IN: 
	  1   2        3          4 
	 sz  ad  exposureprice winprice 
	 OUT: 
	  1   2     3            4                 5 
	 sz  ad  cnt_exp  sum_exposureprice  sum_winprice 
	 
	 | field | description | 
	 | ----- | ----------- | 
	 | sz    | 
	 | ad    | 
	 | cnt_exp | количество показов | 
	 | sum_exposureprice | сумма exposureprice | 
	 | sum_winprice | сумма winprice | 
	 | sum_expprice/winprice | сумма expprice/winprice | 

12_postf_mail_tn0_url:
-----------

**Обработка смерженного (часы в сутки) потока для google, tn0, url.** 

	 IN: 
	  1   2      3         4 
	 dom path  expprice winprice 
	 OUT: 
	  1   2     3          4          5 
	 dom path cnt_exp  sum_exppr sum_winprice 
	 | field        | description | 
	 | -----        | ----------- | 
	 | dom          | 
	 | path         | 
	 | cnt_exp      | количество показов | 
	 | sum_exppr    | сумма expprice | # <--- это поле предполагается не использовать, будем брать exppr из tn3 
	 | sum_winprice | сумма winprice | 

12_postf_mail_tn1:
-----------

**Обработчик смерженного (часы в сутки) потока для google tn1.** 

	 IN 
	  1   2 
	 sz  ad 
	OUT: 
	  1  2      3 
	 sz ad  cnt_clicks 
	 | field | description | 
	 | ----- | ----------- | 
	 | sz    | 
	 | ad    | 
	 | cnt_clicks | количество кликов  | 

12_postf_mail_tn3:
-----------

**Обработчик смерженного (часы в сутки) потока для google tn3.** 

	 Добавляем колонку про сессии: СЕКУНДЫ начала сессии КУКИ на ДОМЕНЕ, 
	 и колонку "признак засчитываемого просмотра страницы" - незасчитываются повторные просмотры одной и тойже страницы с интервалом менее 10 сук. 
	 На входе поток за сутки (т.е период начало и конец которого можно считать разрывом сессии), лучше с 04-00 по 04-00. 
	 отсортрованный по КУКЕ и ВРЕМЕНИ. 
	 OUT: 
	 
	 | field | description | 
	 | ----- | ----------- | 
	 |1 uid   | 
	 |2 sec   | 
	 |3 sz    | 
	 |4 pz    | 
	 |5 bt    | 
	 |6 exposure_price |изза ad не используется| 
	 |7 ad      | 

 |8 expid 

	 |9 domain  | 
	 |10 path    | 
	 |11 sestart | секунды начала сессии   | 
	 |12 isview  | просмотр засчитан (1/0) | 

12_postf_mail_tn3_url:
-----------

**> IN:** 

	  1   2        3         4             5       6          7 
	 dom path  cnt_bids  sum_expprice3  cnt_exp sum_winpr sum_exppr0 

12_postf_net_tn0:
-----------

**Обработчик смерженного (часы в сутки) потока для net tn0** 
net tn0 - это показы, а также "биды" (сетевые показы - выигранные биды) 

	 Добавляем колонку про сессии: СЕКУНДЫ начала сессии КУКИ на ДОМЕНЕ, 
	 и колонку "признак засчитываемого просмотра страницы" - незасчитываются повторные просмотры одной и тойже страницы с интервалом менее 10 сук. 
	 На входе поток за сутки (т.е период начало и конец которого можно считать разрывом сессии), лучше с 04-00 по 04-00. 
	 отсортрованный по КУКЕ и ВРЕМЕНИ. 
	 OUT: 
	 
	 | field | description | 
	 | ----- | ----------- | 
	 |1 uid   | 
	 |2 sec   | 
	 |3 sid   | 
	 |4 sz    | 
	 |5 pz    | 
	 |6 bt    | 
	 |7 exposureprice | 
	 |8 secondprice | 
	 |9 expprice/(secondprice) | 
	 |10 ad      | 
	 |11 network | 
	 |12 domain  | 
	 |13 path    | 
	 |14 sestart | секунды начала сессии   | 
	 |14 isview  | просмотр засчитан (1/0) | 

12_postf_net_tn0_url:
-----------

**По урлам.** 
Обработчик смерженного (часы в сутки) потока для net_tn0_url 
из показов взяты только сетевые, разбивки на сети не делаем 
для net 0 url - есть prefilter, см выход с него 

	 OUT: 
	 | field            | decr | 
	 |----------------  | ---- | 
	 | dom              | 
	 | path             | 
	 | cnt              | 
	 | sum_expprice     | 
	 | sum_secondprice  | сумма secondprice | 

12_postf_net_tn1:
-----------

**Обработчик смерженного (за сутки) потока для net tn1 (клики).** 

	 IN 
	  1   2   3 
	 sid  sz  ad 
	 OUT: 
	 | field | description | 
	 | ----- | ----------- | 
	 | sid   |             | 
	 | sz    | 
	 | ad    | 
	 | cnt_clicks | количество кликов  | 
	  1  2   3      4 
	 sid sz ad  cnt_clicks 

12_postf_ssp_tn0:
-----------

**> IN:** 

	  1   2  3         4          5 
	 sid sz  ad  exposureprice winprice 
	 OUT: 
	  1   2  3     4              5               6           7 
	 sid sz  ad  cnt_exp  sum_exposureprice  sum_winprice sum_exp_div_win 
	 
	 | field | description | 
	 | ----- | ----------- | 
	 | sid   | 
	 | sz    | 
	 | ad    | 
	 | cnt_exp | количество показов | 
	 | sum_exposureprice | сумма exposureprice | 
	 | sum_winprice | сумма winprice | 
	 | sum_expprice/winprice | сумма expprice/winprice | 

12_postf_ssp_tn0_url:
-----------

**> IN:** 

	  1   2      3           4          5 
	 dom path  cnt(exp)  sum(exppr) sum(winprice) 
	 OUT: 
	  1   2     3            4          5 
	 dom path cnt(exp)   sum(exppr) sum(winprice) 

12_postf_ssp_tn1:
-----------

**> IN** 

	  1   2  3 
	 sid sz  ad 
	 OUT: 
	  1  2  3        4 
	 sid sz ad  cnt_clicks 
	 | field | description | 
	 | ----- | ----------- | 
	 | sid   | 
	 | sz    | 
	 | ad    | 
	 | cnt_clicks | количество кликов  | 

12_postf_ssp_tn3:
-----------

**> Добавляем колонку про сессии: СЕКУНДЫ начала сессии КУКИ на ДОМЕНЕ,** 

	 и колонку "признак засчитываемого просмотра страницы" - не засчитываются повторные просмотры одной и той же страницы с интервалом менее 10 сек. 
	 На входе поток за сутки (т.е период начало и конец которого можно считать разрывом сессии), лучше с 04-00 по 04-00. 
	 отсортрованный по КУКЕ и ВРЕМЕНИ. 
	 OUT: 
	 
	 | field   | description | 
	 | ------- | ----------- | 
	 |1  uid   | 
	 |2  sec   | 
	 |3  sid   | 
	 |4  sz    | 
	 |5  pz    | 
	 |6  bt    | 
	 |7  exposure_price |изза ad не используется| 
	 |8  ad      | 
	 |9  expid   | 
	 |10 domain  | 
	 |11 path    | 
	 |12 sestart | секунды начала сессии   | 
	 |13 isview  | просмотр засчитан (1/0) | 

12_postf_ssp_tn3_toolbar:
-----------

**> IN:** 

	  1   2  3  4   5   6 
	 sid id wm d2l dom path 
	 out 
	  1   2  3  4   5   6   7 
	 sid id wm d2l dom path cnt (cnt-количество одинаковых строк) 

12_postf_ssp_tn3_url:
-----------

**> IN:** 

	  1    2        3         4              5        6          7 
	 dom  path  cnt_bids  sum_expprice3  cnt_exp  sum_winpr  sum_exppr0 

12_pref_google_tn3_url:
-----------

**hour_file format:** 

	  1   2      3  4  5       6       7    8    9   10 
	 uid second sz pz bt exposureprice ad expid dom path 

      1   2      3         4               5       6           7 
out: dom path bids(cnt) sum(expprice)  exp(cnt)  sum(winpr) sum(exppr0) 

12_pref_mail_tn3_url:
-----------

**hour_file format:** 

	  1   2      3  4  5       6       7    8    9   10 
	 uid second sz pz bt exposureprice ad expid dom path 

      1   2      3         4               5       6           7 
out: dom path bids(cnt) sum(expprice)  exp(cnt)  sum(winpr) sum(exppr0) 

12_pref_net_tn0_url:
-----------

**пре-фильтр для net 0 url** 
По урлам. 
net tn0 - это показы, а также "биды" (сетевые показы - выигранные биды) 

12_pref_ssp_tn3_toolbar:
-----------

**> IN:** 

	       1    2     3  4  5  6    7    8  9     10  11  12          13 
	 OUT: uid second sid sz pz bt exppr ad expid dom path id(custom) wm(custom) 
	 out: 
	  1   2  3  4   5   6 
	 sid id wm d2l dom path 

12_pref_ssp_tn3_url:
-----------

**>  1   2      3  4  5  6       7         8  9     10  11   12          13** 

	 uid second sid sz pz bt exposureprice ad expid dom path  id(custom) wm(custom) 

      1   2      3        4             5       6            7 
out: dom path bids(cnt) sum(exppr3) exp(cnt)  sum(winpr) sum(exppr0) 

14_BIDS:
-----------

**total(tn3) ----( ./14_process )---> bids(tn3)** 
параметр: 
job=${1:?Job!} # google 
для разных $job разные источники: 
script может отличаться для разных job: 

14_process_google:
-----------

**версия чистого перла N2** 
Making table "bids" (tn3). 

	 For google. 
	 in (from tn3 total.gz): 
	  1   2     3   4  5       6        7    8    9   10    11     12 
	 uid second sz pz  bt exposureprice ad expid dom path sestart isview 
	 OUT: sz, ad, bids, domain. 
	 
	 | field  | description | 
	 | ------ | ----------- | 
	 | sz     | 
	 | ad     | 
	 | bids   | количество бидов | 
	 | domain | домен (как внешний ключ для объединения с данными, сгруппированными по доменам ) | 
	     Прим. 
	      Поле "количество бидов при exposureprice>0" (posibids) отсутствует, потому что 
	      оно равно значению поля bids, за исключением случаев, когда ad=0. В этом случае оно равно 0. 

14_process_mail:
-----------

**версия чистого перла N2** 
Making table "bids" (tn3). 

	 For google. 
	 in (from tn3 total.gz): 
	  1   2     3   4  5       6        7    8    9   10    11     12 
	 uid second sz pz  bt exposureprice ad expid dom path sestart isview 
	 OUT: sz, ad, bids, domain. 
	 
	 | field  | description | 
	 | ------ | ----------- | 
	 | sz     | 
	 | ad     | 
	 | bids   | количество бидов | 
	 | domain | домен (как внешний ключ для объединения с данными, сгруппированными по доменам ) | 
	     Прим. 
	      Поле "количество бидов при exposureprice>0" (posibids) отсутствует, потому что 
	      оно равно значению поля bids, за исключением случаев, когда ad=0. В этом случае оно равно 0. 

14_process_net:
-----------

**Making table "bids" for net (tn0).** 

	 in (from tn0 total.gz): 
	 OUT="sz ad is_posibid is_network bids domain" # comment 
	 
	 | field  | description | 
	 | ------ | ----------- | 
	 | sid    | 
	 | sz     | 
	 | ad     | 
	 | is_posibid | ставки >0 | 
	 | is_network | сетевые показы (выинранные биды) | 
	 | bids   | количество бидов | 
	 | domain | домен (как внешний ключ для объединения с данными, сгруппированными по доменам ) | 
	 сортировака -n -k1,1 -k2,2 -k3,3 -k4,4 

14_process_ssp:
-----------

**версия чистого перла N2** 
Making table "bids" (tn3). 

	 For ssp. 
	 in (from tn3 total.gz): 
	  1   2      3   4  5  6        7       8   9    10   11    12      13 
	 uid second sid  sz pz bt exposureprice ad expid dom path sestart isview 
	 OUT: sz, ad, bids, domain. 
	 
	 | field  | description | 
	 | ------ | ----------- | 
	 | sid    | 
	 | sz     | 
	 | ad     | 
	 | bids   | количество бидов | 
	 | domain | домен (как внешний ключ для объединения с данными, сгруппированными по доменам ) | 
	     Прим. 
	      Поле "количество бидов при exposureprice>0" (posibids) отсутствует, потому что 
	      оно равно значению поля bids, за исключением случаев, когда ad=0. В этом случае оно равно 0. 

15_process:
-----------

**Making table "sessions" (tn3).** 

	 in: 
	 ожидаются поля: dom, uid, sestart 
	 OUT: dom, sessions_count, views_count 
	 
	 | field          | description | 
	 | -------------- | ----------- | 
	 | dom            | 
	 | sessions_count | 
	 | views_count    | 

15_SESSIONS:
-----------

**total.gz (tn3)-----(./15_process)--->sessions.gz** 
параметры: 
job=${1:?JOB!} # job 

16_process:
-----------

**Сочетания pz+bt по sz.** 

	 in (у google): 
	  1   2          3  4  5       6        7   8     9   10    11      12 
	 uid second     sz pz  bt exposureprice ad expid dom path sestart isview 
	 или такой (у ssp) 
	  1   2      3  4  5   6        7       8    9   10   11    12      13 
	 uid second sid sz pz  bt exposureprice ad expid dom path sestart isview 
	 или такой (у net) 
	  1   2     3   4  5   6        7       8   9   10    11      12 
	 uid second sid sz pz  bt exposureprice ad dom path sestart isview 
	 out: 
	 
	 | field | 
	 | ----- | 
	 | [sid] | для net, ssp - это поле есть, для google - нет 
	 | sz    | 
	 | pz    | 
	 | bt    | 
	 | cnt   | используется в szvars_30days - для сумм по pz и bt 
	 оно является = количество строк из google tn3 total для sz+pz+bt 
	 т.е. количество бидов по sz+pz+bt 

16_PZBT:
-----------

**total.gz (tn3) ----(./16_process)----> pzbt.gz** 

17_process:
-----------

**urls.** 
IN: 
google: 
 1   2     3   4  5       6        7   8     9   10    11     12 
uid second sz pz  bt exposureprice ad expid dom path sestart isview 
OR 
net: 
uid second sid sz pz bt exposureprice secondprice divexpsecond ad network dom path sestart isview 
OR: 
ssp: 
uid second sid sz pz bt exposureprice ad expid dom path sestart isview 

	 sorted by UID,SECOND: 

when uid was changed: 
sessions counters: 
sessions_end counter: 

	OUT: 
	 dom, ref, sessions_count, sessions_ends 
	 
	 | field          | description | 
	 | -------------- | ----------- | 
	 | dom  	     | 
	 | path 	     | 
	 | sessions_count | количество сессий, где dom+url участвовал | 
	 | sessions_ends  | количество сессий, где он был последним.  | 

17_URLS:
-----------

**total.gz (tn3) ----(./17_process)----> urls.gz** 
параметр: 
job=${1:?Job!} # google 

18_UIDSZ:
-----------

**из total(tn3) ----> uidsz.gz ( Куки на сайтзонах. Сочетания uid+sz )** 

? Возможно потом придется переделать на "куки на sz+ad" , а из него уже делать куки на sz.(?) 

	OUT: 
	 uid, [sid,] sz. (уникальные комбинации) 
	 сортировка по uid [sid,] и sz. 

19_process:
-----------

**Суммы счетчиков за N дней по файлам urls.gz** 
параметр (имя вх файла): 
src_file=${1:?Src file!} # f.e...../urls.gz 
параметр (job): 
job=${2:?Job!} # f.e. google 
парам: 
typenum=${3:?typenum!} 
параметр (days): 
mimN=${4:? minN Days!} # f.e. 5. За сколько дней суммировать minimum 

	 out 
	 
	 | field     | 
	 | --------- | 
	 | dom       | 
	 | path      | 
	 | sessions  | 
	 | sess_ends | 

19_URLS30DAYS:
-----------

**> test test test** 
urls.gz -----(19_process)---------->scala.urls30days.gz 

20_DOM30DAYS:
-----------

**urls30days.gz -----( удаляет поле path )----------> dom30days.gz** 

21_DOMGR:
-----------

**doms30days.gz -----( ...dm1 job via ssh ...)---->dom_gr.txt , quant.gz** 
Запускать 1 поток! 

21_process:
-----------

**** 

22_DOMGR7DAYS:
-----------

**dom_gr.txt за 7 дней -----> dom_gr7days.gz** 
Для домена выбирает кластер: последний из самых частых за 7 дней. 
OUT: 
dom, group (a.k.a. cluster) 

25_process:
-----------

**urls30days + groups = urls_group** 

	 Проходит по файлу urls30days (смерженные урлы за 30 дней), глядя в группы от Димы, 
	  для каждого урла печатает dom*path*RV, где RV = $groups_group * 10 + $subgroup_number + ( Sessions < 50 ? 100 : 0 ) 

если в quant нет такой группы, то подгруппа=0 

	 out: 
	 
	 | field | 
	 | ----- | 
	 | dom   | 
	 | path  | 
	 | group | 

25_URLGR:
-----------

**dom_gr7days.gz ------(./25_process)------->url_groups.gz** 

27_diff:
-----------

**различия между предыдущим и текущим url_groups** 
На входе смерженный поток dom * path * group * "curr"|"prev" 
Если в curr появилась запись (dom+path) или значение group изменилось, то - insert 
Если в curr исчезла запись (dom+path) - то delete 
out: 
dom * path * curr_group * "insert"|"delete" 

27_URLGR_DIFF:
-----------

**urls_groups.gz [вчера]+[сегодня] -----(27_process---23_diff)------> url_groups_diff.gz** 

28_process:
-----------

**** 

28_URL_CAT:
-----------

**** 

29_diff:
-----------

**различия между предыдущим и текущим url_cat** 
На входе смерженный поток 

	              1                  2          3 
	  000a.ru/a/best_year/2015/40 * 177 * "curr"|"prev" 

Если в curr появилась запись (cat+url) или значение cat изменилось, то - insert 
Если в curr исчезла запись (cat+url) - то delete 
out: 
url * curr_cat * "insert"|"delete" 

30_url_cat_to_send:
-----------

**подготовить и отдать на засылку в базу url_cat_diff** 

32_COPY_TRAITS:
-----------

**hist7 / traits files -----( copying ) ---> ../RESULT/traits_copy** 

33_process_google:
-----------

**пересечение uidsz.gz за 30 дней с файлами трейтов predict_add.gz, predict_gr_v2.gz, ctr10gr.gz, ctr11gr.gz** 

	 gender (predict_add.gz): 
	 format: 1480566*11 
	  11 - 2.54 муж 
	  12 - 2.55 жен 
	 age (predict_gr_v2.gz): 
	 format: 10000022992*1 
	  1 - 2.56 young (12-24) 
	  2 - 2.57 adult (25-44) 
	  3 - 2.60 old (45+) 
	 ctr: (ctr10gr.gz ) 
	 format: 25777293644*29926*0.3*1.00247e-05*10 (see field 5) 
	 1 - most active 
	 10 - minumum activity 
	 ctr11gr.gz 
	 format: 10039181520*1*0*0*0 

all cookies in 1-st field - in 11 group (inactive ) 

	 auditory.gz 
	 format: 
	 uid * group 
	 sorted: 1,1n 
	 filtered_traits.txt.gz 
	 format: 
	 uid \t	trait	... (other fields) 
	 мержит все uidsz за 30 дней, уникалит и подает на вход скриту 
	 out 
	  sz * all * male * fem * age1 * age2 * age3 * ctr1 * ctr2 * ctr3 * ... ctr11 * auditory01 * aud02 * aud03 * ... auditory29 * buyer * trash 
	  1     2      3      4     5     6      7       8     9       10         18      19           20      21            47        48      49 

33_process_mail:
-----------

**пересечение uidsz.gz за 30 дней с файлами трейтов predict_add.gz, predict_gr_v2.gz, ctr10gr.gz, ctr11gr.gz** 

	 gender (predict_add.gz): 
	 format: 1480566*11 
	  11 - 2.54 муж 
	  12 - 2.55 жен 
	 age (predict_gr_v2.gz): 
	 format: 10000022992*1 
	  1 - 2.56 young (12-24) 
	  2 - 2.57 adult (25-44) 
	  3 - 2.60 old (45+) 
	 ctr: (ctr10gr.gz ) 
	 format: 25777293644*29926*0.3*1.00247e-05*10 (see field 5) 
	 1 - most active 
	 10 - minumum activity 
	 ctr11gr.gz 
	 format: 10039181520*1*0*0*0 

all cookies in 1-st field - in 11 group (inactive ) 

	 auditory.gz 
	 format: 
	 uid * group 
	 sorted: 1,1n 
	 filtered_traits.txt.gz 
	 format: 
	 uid \t	trait	... (other fields) 
	 мержит все uidsz за 30 дней, уникалит и подает на вход скриту 
	 out 
	  sz * all * male * fem * age1 * age2 * age3 * ctr1 * ctr2 * ctr3 * ... ctr11 * auditory01 * aud02 * aud03 * ... auditory29 * buyer * trash 
	  1     2      3      4     5     6      7       8     9       10         18      19           20      21            47        48      49 

33_process_net:
-----------

**Для net.** 
пересечение uidsz.gz за 30 дней с файлами трейтов predict_add.gz, predict_gr_v2.gz, ctr10gr.gz, ctr11gr.gz 
v2, надеемся на ускорение. 

	 gender (predict_add.gz): 
	 format: 1480566*11 
	  11 - 2.54 муж 
	  12 - 2.55 жен 
	 age (predict_gr_v2.gz): 
	 format: 10000022992*1 
	  1 - 2.56 young (12-24) 
	  2 - 2.57 adult (25-44) 
	  3 - 2.60 old (45+) 
	 ctr: (ctr10gr.gz ) 
	 format: 25777293644*29926*0.3*1.00247e-05*10 (see field 5) 
	 1 - most active 
	 10 - minumum activity 
	 ctr11gr.gz 
	 format: 10039181520*1*0*0*0 

all cookies in 1-st field - in 11 group (inactive ) 

	 auditory.gz 
	 format: 
	 uid * group 
	 sorted: 1,1n 
	 filtered_traits.txt.gz 
	 format: 
	 uid \t	trait	... (other fields) 
	 мержит все uidsz за 30 дней, уникалит и подает на вход скриту 
	 OUT: 
	 sid *  sz * all * male * fem * age1 * age2 * age3 * ctr1 *  ... ctr11 * auditory01 *  ... auditory29 * buyer * trash 
	   1     2     3     4     5      6      7      8     9            19        20                 48        49     50 

33_process_ssp:
-----------

**Для ssp.** 
пересечение uidsz.gz за 30 дней с файлами трейтов predict_add.gz, predict_gr_v2.gz, ctr10gr.gz, ctr11gr.gz 
v2, надеемся на ускорение. 

	 gender (predict_add.gz): 
	 format: 1480566*11 
	  11 - 2.54 муж 
	  12 - 2.55 жен 
	 age (predict_gr_v2.gz): 
	 format: 10000022992*1 
	  1 - 2.56 young (12-24) 
	  2 - 2.57 adult (25-44) 
	  3 - 2.60 old (45+) 
	 ctr: (ctr10gr.gz ) 
	 format: 25777293644*29926*0.3*1.00247e-05*10 (see field 5) 
	 1 - most active 
	 10 - minumum activity 
	 ctr11gr.gz 
	 format: 10039181520*1*0*0*0 

all cookies in 1-st field - in 11 group (inactive ) 

	 auditory.gz 
	 format: 
	 uid * group 
	 sorted: 1,1n 
	 filtered_traits.txt.gz 
	 format: 
	 uid \t	trait	... (other fields) 
	 мержит все uidsz за 30 дней, уникалит и подает на вход скриту 
	 OUT: 
	 sid *  sz * all * male * fem * age1 * age2 * age3 * ctr1 * ... ctr11 * auditory01 *  ... auditory29 * buyer * trash 
	   1    2     3      4     5      6      7      8     9           19        19                 48        49      50 

33_SZ_TRAITS_30DAYS:
-----------

**uidsz [за 30 дней] + traits_copy/.. ------(33_process)--------->sz_traits_30days.gz** 

34_url_traits:
-----------

**> gender (predict_add.gz):** 

	 format: 1480566*11 
	  11 - 2.54 муж 
	  12 - 2.55 жен 
	 filtered_traits.txt.gz 
	 format: 
	 uid \t	trait	... (other fields) 
	 мержит все uidurl за 30 дней, уникалит и подает на вход скриту 
	 OUT: 
	 dom *  path * all * male * fem * buyer * trash 
	   1     2      3     4      5      6       7 

35_DOM_NET:
-----------

**For: net / tn0 / total.gz ----( 35_process )------> dom_net.gz** 

35_process:
-----------

**** 

	 out: 
	 | field | description | 
	 |-------|-------------| 
	 | dom   | домен       | 
	 | net   | сеть, в которой были просмотры | 
	 
	 sorted: 1,1  2,2n 

37_process:
-----------

**net / 0 / total ----> views** 

	 OUT: 
	 | field            | decr | 
	 |----------------  | ---- | 
	 | sid              | 
	 | sz               | 
	 | is_network?      | = 1/0                       | 
	 | exposures        | сумма просмотров            | 
	 | sum_expprice     | сумма exposureprice         | 
	 | sum_secondprice  | сумма secondprice           | 
	 | sum_divexpsecond | сумма отношений exppr/secpr | 

37_VIEWS:
-----------

**net tn0 total.gz ----(37_process)-------> views.gz** 

51_process_google:
-----------

**Объединение сумм из отдельных: bids.gz, tn0/.../total.gz, tn1/.../total.gz, sessions.gz** 
 в одну таблицу по sz + ad. 
за 30 дней 
Этот скрипт только для google, ssp. 
смерженные файлы bids.gz: 
30 days bids.gz мержатся по первым двум полям (sz,ad), третье поле (bids_count) суммируется, четвертое (dom) - берется последнее значение по данному ключу: 
(ad нужна для posi_bids - ни равны 0 при ad==0) 

	 OUT: 
	 | field               | description  | 
	 | ------------------- | ------------ | 
	 | 1  google_sid:sz    | (google_sid всегда одинаковый) | 
	 | 2  domain           | 
	 | 3  dom_gr	   | группа домена  | 
	 | 4  ctr              | клики/показы  | 
	 | 5  winbids          | процент выигранных нами бидов (просмотры/биды) (%) | 
	 | 6  avg_session      | ср длина пользов. сессии на домене (по домену)     | 
	 | 7  all_uids         | вего кук на сайтзоне                               | 
	 | 8  avg_winprice     | средний winprice                                   | 
	 | 9  avg_exp_div_win  | среднее отношение expprice/winprice                | 
	 | 10  posibids        | отношение posibids/bids - доля бидов с положит ставкой  | 
	 | 11  win_posi_bids   | доля выигранных бидов от тех, где ставка>0              | 
	 | 12 pzbt_count       | количество сочетаний bt-pz                              | 
	 | 13..16 in_nets      | 4 поля признаков сетей, в кот. входит сайтзона 1/0 (по домену)  | 
	 | 17..18 gender       | 2 поля male, female (%)							| 
	 | 19..21 age          | 3 поля yang, adult, old (%)						| 
	 | 22..32 ctr groups   | 11 полей ctr 1-11 (%)							| 
	 | 33..61 auditory     | 29 полей auditory (%)							| 
	 | 62 buyers	   | трейт покупатели (%)							| 
	 | 63..64 pz bids      | 2 поля: процент бидов по pz / к бидам по sz в (%), список pz - в 00_conf.txt  | 
	 | 65..104 bt bids     | 40 полей: процент бидов по bt / к бидам по sz в (%)                             | 
	 | 105 avg_exppr3_all  | средний exposure price по tn3 | 
	 | 106 avg_exppr3_finsess | средний exposure price по первым в сессии по tn3 | 
	 | 107 avg_exppr0      | средний exp price по показам | 
	 sz * all * male * fem * age1 * age2 * age3 * ctr1 * ctr2 * ctr3 * ... ctr11 * auditory01 * aud02 * aud03 * ... auditory30 * trash 
	 1     2      3      4     5     6      7       8     9       10         18      19           20      21            48        49 

51_process_mail:
-----------

**Объединение сумм из отдельных: bids.gz, tn0/.../total.gz, tn1/.../total.gz, sessions.gz** 
 в одну таблицу по sz + ad. 
за 30 дней 
Этот скрипт только для google, ssp. 
смерженные файлы bids.gz: 
30 days bids.gz мержатся по первым двум полям (sz,ad), третье поле (bids_count) суммируется, четвертое (dom) - берется последнее значение по данному ключу: 
(ad нужна для posi_bids - ни равны 0 при ad==0) 

	 OUT: 
	 | field               | description  | 
	 | ------------------- | ------------ | 
	 | 1  google_sid:sz    | (google_sid всегда одинаковый) | 
	 | 2  domain           | 
	 | 3  dom_gr	   | группа домена  | 
	 | 4  ctr              | клики/показы  | 
	 | 5  winbids          | процент выигранных нами бидов (просмотры/биды) (%) | 
	 | 6  avg_session      | ср длина пользов. сессии на домене (по домену)     | 
	 | 7  all_uids         | вего кук на сайтзоне                               | 
	 | 8  avg_winprice     | средний winprice                                   | 
	 | 9  avg_exp_div_win  | среднее отношение expprice/winprice                | 
	 | 10  posibids        | отношение posibids/bids - доля бидов с положит ставкой  | 
	 | 11  win_posi_bids   | доля выигранных бидов от тех, где ставка>0              | 
	 | 12 pzbt_count       | количество сочетаний bt-pz                              | 
	 | 13..16 in_nets      | 4 поля признаков сетей, в кот. входит сайтзона 1/0 (по домену)  | 
	 | 17..18 gender       | 2 поля male, female (%)							| 
	 | 19..21 age          | 3 поля yang, adult, old (%)						| 
	 | 22..32 ctr groups   | 11 полей ctr 1-11 (%)							| 
	 | 33..61 auditory     | 29 полей auditory (%)							| 
	 | 62 buyers	   | трейт покупатели (%)							| 
	 | 63..64 pz bids      | 2 поля: процент бидов по pz / к бидам по sz в (%), список pz - в 00_conf.txt  | 
	 | 65..104 bt bids      | 40 полей: процент бидов по bt / к бидам по sz в (%)                             | 
	 sz * all * male * fem * age1 * age2 * age3 * ctr1 * ctr2 * ctr3 * ... ctr11 * auditory01 * aud02 * aud03 * ... auditory30 * trash 
	 1     2      3      4     5     6      7       8     9       10         18      19           20      21            48        49 

51_process_net:
-----------

**Объединение сумм из отдельных: bids.gz, tn0/.../total.gz, tn1/.../total.gz, sessions.gz** 
 в одну таблицу по sz [+ad]. 
за 30 дней 
Этот скрипт только для net. 
смерженные файлы bids.gz: 
format net/0/bids: sid, sz, ad, is_posibid, is_network, bids, domain 

	 OUT: 
	 | field               | description  | 
	 | ------------------- | ------------ | 
	 | 1  sid:sz           | 
	 | 2  domain           | 
	 | 3  dom_gr           | доменная группа | 
	 | 4  ctr              | клики/показы | 
	 | 5  winbids          | процент выигранных нами бидов (просмотры/биды) (%) | 
	 | 6  avg_session      | ср длина пользов. сессии на домене (по домену)     | 
	 | 7  all_uids         | вего кук на сайтзоне                               | 
	 | 8  avg_winprice     | средний winprice                                   | 
	 | 9  avg_exp_div_win  | среднее отношение expprice/winprice                | 
	 | 10 posibids         | отношение posibids/bids - доля бидов с положит ставкой  | 
	 | 11  win_posi_bids   | доля выигранных бидов от тех, где ставка>0              | 
	 | 12 pzbt_count       | количество сочетаний bt-pz                              | 
	 | 13..16 in_nets      | 4 поля признаков сетей, в кот. входит сайтзона 1/0 (по домену)     | 
	 | 17..18 gender       | 2 поля: male,female (%)						| 
	 | 19..21 age          | 3 поля: yang,adult,old (%) 					| 
	 | 22..32 ctr groups   | 11 полей ctr 1-11 (%)						| 
	 | 33..61 auditory     | 29 полей auditory 1-29 (%)						| 
	 | 62 buyers           | 1 поле трейт покупателей (вместо  30-го поля auditory) (%)		| 
	 | 63..64 pz bids      | процент бидов по pz / к бидам по sz в (%), список pz - в 00_conf.txt  | 
	 | 65..104 bt bids     | 40 полей процент бидов по bt / к бидам по sz в (%)                    | 
	 | 105		   | "NA" (средний exposure price по бидам) 
	 | 106                 | "NA" (средний exposure price по первым в сессии по бидам) 
	 | 107                 | средний expprice по сетевым показам 

51_process_ssp:
-----------

**Объединение сумм из отдельных: bids.gz, tn0/.../total.gz, tn1/.../total.gz, sessions.gz** 
 в одну таблицу по sz + ad. 
за 30 дней 
Этот скрипт только для ssp. 
смерженные файлы bids.gz: 
30 days bids.gz мержатся по первым 3 полям (sid,sz,ad), 4 поле (bids_count) суммируется, 5 (dom) - берется последнее значение по данному ключу: 
(ad нужна для posi_bids - ни равны 0 при ad==0) 

	 OUT: 
	 | field               | description  | 
	 | ------------------- | ------------ | 
	 | 1  sid:sz           |              | 
	 | 2  domain           | 
	 | 3  dom_gr	   | группа домена | 
	 | 4  ctr              | клики/показы  | 
	 | 5  winbids          | процент выигранных нами бидов (просмотры/биды) (%) | 
	 | 6  avg_session      | ср длина пользов. сессии на домене (по домену)     | 
	 | 7  all_uids         | вего кук на сайтзоне                               | 
	 | 8  avg_winprice     | средний winprice                                   | 
	 | 9  avg_exp_div_win  | среднее отношение expprice/winprice                | 
	 | 10  posibids        | отношение posibids/bids - доля бидов с положит ставкой  | 
	 | 11  win_posi_bids   | доля выигранных бидов от тех, где ставка>0              | 
	 | 12 pzbt_count       | количество сочетаний bt-pz                              | 
	 | 13..16 in_nets      | 4 поля признаков сетей, в кот. входит сайтзона 1/0 (по домену)    | 
	 | 17..18 gender       | 2 поля: male,female (%)						| 
	 | 19..21 age          | 3 поля: yang,adult,old (%) 					| 
	 | 22..32 ctr groups   | 11 полей ctr 1-11 (%)						| 
	 | 33..61 auditory     | 29 полей auditory 1-29 (%)						| 
	 | 62 buyers           | 1 поле трейт покупателей (вместо  30-го поля auditory) (%)		| 
	 | 63..64 pz bids      | 2 поля: процент бидов по pz / к бидам по sz в (%), список pz - в 00_conf.txt  | 
	 | 65..104 bt bids     | 40 полей: процент бидов по bt / к бидам по sz в (%)                           | 
	 | 105 avg_exppr3_all  | средний exposure price по tn3 | 
	 | 106 avg_exppr3_finsess | средний exposure price по первым в сессии по tn3 | 
	 | 107 avg_exppr0      | средний exposure price по показам | 

51_SZVARS_30DAYS:
-----------

**bids.gz , totals... ----> sz_vars_30days.gz** 

52_urlvars_30days_google:
-----------

**** 
 1    2          3               4          5       6       7         8      9     10               11              12 
dom  path  avg_expprice0, $avg_winprice, $profit, $male, $female, $buyerss  flag  int(m/(m/f))%  winrate_x1000  avg_expprice3 

52_urlvars_30days_mail:
-----------

**скопировано как есть из ssp, после того как там внесены изменения** 
про expprice по показам. 
Не проверялось в работе. 
 1    2         3               4          5       6       7         8      9     10               11	        12 
dom  path  avg_expprice0, $avg_winprice, $profit, $male, $female, $buyerss  flag  int(m/(m/f))%  winrate_x1000   avg_expprice3 

52_urlvars_30days_mail.OLD:
-----------

**** 
 1    2          3               4          5       6       7         8      9     10               11              12 
dom  path  avg_expprice0, $avg_winprice, $profit, $male, $female, $buyerss  flag  int(m/(m/f))%  winrate_x1000  avg_expprice3 

52_urlvars_30days_net:
-----------

**** 
 1    2         3               4          5       6       7         8      9     10 
dom  path  avg_expprice, $avg_winprice, $profit, $male, $female, $buyerss  flag  int(m/(m/f))% 

52_urlvars_30days_ssp:
-----------

**** 
 1    2         3               4          5       6       7         8      9     10               11	        12 
dom  path  avg_expprice0, $avg_winprice, $profit, $male, $female, $buyerss  flag  int(m/(m/f))%  winrate_x1000   avg_expprice3 

52_urlvars_diff:
-----------

**Usage:** 
1) для получения дифа по двум файлам, нужно пустить на STDIN смерженные по dom+path их потоки, 
 добавив дополнительную колонку к каждому потоку: "P" - для предыдущих значений, "C" - для текущих. 
 например, 
 file1=../1460/RESULT/10/2015-08-03/net_sites/0/url_vars_30days.gz 
 file2=../1460/RESULT/10/2015-08-10/net_sites/0/url_vars_30days.gz 
 LANG=POSIX sort -t\* -m <(zcat "$file1" | perl -lne'print $_."*P"')  <(zcat "$file2" | perl -lne'print $_."*C"') -k1,1 -k2,2 | ./52_urlvars_diff" 
2) для получения начального дифа (для первой загрузки ) - пустить на вход поток этого файла, без добавления колонки. 
 zcat ~/1460/RESULT/10/2015-08-03/net_sites/0/url_vars_30days.gz | ./52_urlvars_diff | head 
Формат файлов должен соответствовать описанию ниже. Разделитель - *. 
если поле Version пустое, то считается "C"-current. Для случаев когда дифф делается по одному файлу, для начальной загрузки. 

53_sz_vars_conv:
-----------

**На входе поток колонок со значениями (sz_vars), разделитель *.** 

53_sz_vars_conv_DEL781:
-----------

**На входе поток колонок со значениями (sz_vars), разделитель *.** 

55_process:
-----------

**** 

55_SZGR:
-----------

**sz_vars_30days.gz -----( ...dm1 job via ssh ...)----> clust_sz_1.txt** 
Запускать 1 поток! 

63_get_toolbars:
-----------

**** 

63_toolbar_urls:
-----------

**** 

64_toolbar_percents:
-----------

**на вход подать поток с total-from-2015-10-16T00-7-days.gz** 

65_toolbar_rule:
-----------

**file like a: total-from-YYYY-MM-DDTHH-N-days.gz** 
187544*5*hotelnews.ru*266*5*583*google.ru:1,googleusercontent.com:1,hotelnews.ru:579,prlog.ru:1,yandex.net:1 

93_LOGS_TAIL:
-----------

**оставляет N последних записей в логах** 

95_REMOVE_EMPTY:
-----------

**Удаление пустых файлов.** 

95_REMOVE_OLD_HOURS:
-----------

**Удаление старых часовых файлов.** 

95_REMOVE_OLD_urls30days:
-----------

**Удаление старых файлов urls30days.gz потому что они большие.** 

96_REMOVE_OLD_ALL:
-----------

**Удаление старых данных.** 

Dom.pm:
-----------

**Перевод домена (1-параметр, строка) в домен 2-го уровня** 
Второй параметр - опции { d3l => ["regexp1","regexp2",...] } 
 попадающие под регексп - переводятся в 3-й уровень. 
Для всех производится очистка от порта, протокола, пути, пробелов 

set_DELETE_url_vars_for_bidder.pl:
-----------

**Только для FIX (2015.09.15)** 
удалить, если в базе указанный урл (поле 1) имеет категорию из списка --del-cats 
на входе поток формата: url * ... 

ufix-find-files.sh:
-----------

**для фикса базы** 

