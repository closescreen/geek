00_ABOUT:
-----------

**"Подготовка данных для кластеризации сайтзон для байесовского биддера"** 

01_START_ALL:
-----------

**Запуск всех скриптов.** 

01_STOP_ALL:
-----------

**Остановка всех скриптов.** 
Создается файл 01_STOP_ALL.stop, который перед запуском 01_START_ALL нужно удалить. 

10_google_tn0:
-----------

**сбор из hl typenum==0 (показы) , sid==187537 (google)** 

	 отбрасываются нулевые sz,uid 
	 OUT: 
	  sz,ad,exposureprice,winprice 
	  сортировка по sz, ad. 

10_google_tn1:
-----------

**сбор из hl typenum==1 (клики) , sid==187537 (google)** 

	 отбрасываются нулевые sz, uid. 
	 поля: sz,ad 
	 сортировка: по sz,ad 

10_google_tn3:
-----------

**сбор из hl typenum==3 (bids), sid==187537 (google)** 

	 отбрасываются нулевые sz,uid. 
	 поля: uid, second, sz, pz, bt, exposureprice, ad, ref 
	 "ref" сплитится на два поля: "dom" и "path" 
	 сортировка: uid, second. 

10_HOURS:
-----------

**history_log -------( jobscript )--------> ../RESULT/10/${day}/${job}/${typenum}/yyyy-mm-ddThh.gz** 

	 Сбор часовых данных из hl. 

Параметры: $job и $typenum. 
Каждой комбинации $job и $typenum соответствует свой скрипт сбора. 
jobscript="./10_${job}_tn${typenum}" 
Если присутствует total.gz то часовые данные не собираются. 
Часы группируются в папки по суткам с 04.00 до 04.00. 
период, за который собрать данные (если их еще нет) 
period="30days" 

10_net-sixty_tn0:
-----------

**сбор из hl typenum==0 (показы) , network==60** 

	 отбрасываются нулевые sz,uid 
	 OUT: 
	  Sz, Ad, Exposureprice, (Winprice||(Secondprice/100_000)) 
	  сортировка по sz, ad. 

10_net-sixty_tn1:
-----------

**сбор из hl typenum==1 (клики) , network=60** 

	 отбрасываются нулевые sz, uid. 
	 OUT: sz, ad 
	 сортировка: по sz,ad 

10_net-sixty_tn3:
-----------

**сбор из hl typenum==3 (bids), network=60** 

	 НЕ ПРИДУМАЛИ КАК ДЕЛАТЬ, ОТЛОЖЕНО 

12_MERGE:
-----------

**Часы (YYYY-MM-HH.gz) ---------( через $posfilter )-----> $job/$tn/total.gz (в сутки)** 
параметр: 
job=${1:? Job! } 
параметр: 
 
Имя скрипта $postfilter зависит от $job и $typenum: 
postfilter="./12_postf_${job}_tn${typenum}" 
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
	 | uid   | 
	 | sec   | 
	 | sz    | 
	 | pz    | 
	 | bt    | 
	 | exposure_price | 
	 | ad      | 
	 | domain  | 
	 | path    | 
	 | sestart | секунды начала сессии   | 
	 | isview  | просмотр засчитан (1/0) | 

14_BIDS:
-----------

**total(tn3) ----( ./14_process )---> bids(tn3)** 
параметр: 
job=${1:?Job!} # google 

14_process:
-----------

**Making table "bids" (tn3).** 

	 in (from tn3 total.gz): 
	  1   2     3   4  5       6        7   8   9     10      11 
	 uid second sz pz  bt exposureprice ad dom path sestart isview 
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

15_process:
-----------

**Making table "sessions" (tn3).** 

	 in: 
	  1   2     3   4  5       6        7   8   9     10      11 
	 uid second sz pz  bt exposureprice ad dom path sestart isview 
	 OUT: dom, sessions_count, views_count 
	 
	 | field          | description | 
	 | -------------- | ----------- | 
	 | dom            | 
	 | sessions_count | 
	 | views_count    | 

15_SESSIONS:
-----------

**total.gz (tn3)-----(./15_process)--->sessions.gz** 
параметр: 
job=${1:?JOB!} 

16_process:
-----------

**Сочетания pz+bt по sz.** 

	 in: 
	  1   2     3   4  5       6        7   8   9     10      11 
	 uid second sz pz  bt exposureprice ad dom path sestart isview 
	 out: 
	 
	 | field | 
	 | ----- | 
	 | sz    | 
	 | pz    | 
	 | bt    | 

16_PZBT:
-----------

**total.gz (tn3) ----(./16_process)----> pzbt.gz** 

17_process:
-----------

**urls.** 

	IN: 
	  1   2     3   4  5       6        7   8   9     10      11 
	 uid second sz pz  bt exposureprice ad dom path sestart isview 
	 sorted by UID,SECOND: 

when uid+sestart was changed: 
sessions counters must be inremented: 
sessions_end counter for last url must be incremented: 

	OUT: 
	 dom, ref, sessions_count, sessions_ends 
	 
	 | field          | description | 
	 | -------------- | ----------- | 
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
	 uid, sz. (уникальные комбинации) 
	 сортировка по uid и sz. 

19_process:
-----------

**Суммы счетчиков за N дней по файлам urls.gz** 
параметр (имя вх файла): 
src_file=${1:?Src file!} # f.e...../urls.gz 
параметр (job): 
job=${2:?Job!} # f.e. google 
параметр (days): 
N=${3:?N Days!} # f.e. 30. За сколько дней суммировать 

	 out 
	 
	 | field     | 
	 | --------- | 
	 | dom       | 
	 | path      | 
	 | sessions  | 
	 | sess_ends | 

19_URLS30DAYS:
-----------

**urls.gz -----(19_process)---------->urls30days.gz** 

20_DOM30DAYS:
-----------

**urls30days.gz -----( удаляет поле path )----------> dom30days.gz** 

21_DOMGR:
-----------

**doms30days.gz -----( ...crawler job via ssh ...)---->dom_gr.gz , quant.gz** 
Запускать 1 поток! 

21_process:
-----------

**** 

22_process:
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

22_URLGR:
-----------

**dom_gr.txt ------(./22_process)------->url_groups.gz** 

23_diff:
-----------

**различия между предыдущим и текущим url_groups** 
На входе смерженный поток dom * path * group * "curr"|"prev" 
Если в curr появилась запись (dom+path) или значение group изменилось, то - insert 
Если в curr исчезла запись (dom+path) - то delete 
out: 
dom * path * curr_group * "insert"|"delete" 

23_process:
-----------

**параметр (job), (здесь не используется)** 
job=${1:? Job! } 
параметр: имя файла-источника (current): 
src=${2:? src file! } 
из файла-источника получет день 
получает предыдущий день 
получает имя (previous) файла-источника 
previous + current --> пускает в один смерженный поток с добавлением последней колонки "curr"|"prev" и подает на вход ./23_diff 

23_URLGR_DIFF:
-----------

**urls_groups.gz [вчера]+[сегодня] -----(23_process---23_diff)------> url_groups_diff.gz** 

30_process:
-----------

**из сумм по sz, ad ---> суммы по sz** 

	 Добавляется колонка: количество бидов при ad!=0, равное кол-ву бидов где была положительная ставка 
	 сессии по домену суммировать нельзя, а нужно оставить одно (первое значение) 

! поля domain в table29 нет, иду туда, добавлять... 

	 in: 
	 Sz, Ad, $exposures, $clicks, Bids, $exppr, $winpr, dom,  $dom_sessions, $dom_views 
	 OUT: 
	  Sz, exposures, clicks, bids, positive_bids, exppr, winpr, Dom, Dom_sessions, Dom_views 
	  ,где 
	    sz - сайтзона (группировка) 
	    exposures - показов 
	    clicks - кликов 
	    bids - бидов всех 
	    positive_bids - бидов при положительной ставке 
	    exppr - exposureprice (ставка) 
	    winpr - winprice 
	    Dom - домен, которому принадлежит сайтзона 
	    Dom_sessions - сессий на домене 
	    Dom_views - просмотров на домене (сумма длин сессий) 

30_SUM:
-----------

**table29 [т.е. суммы по sz и ad] ------( суммирование по sz )------> получаем table30.gz [суммы по sz]** 

	 OUT: 
	   Sz, exposures, clicks, bids, exppr, winpr, dom_sessions, dom_views 
	   (аналогично table29 только суммы не по sz+ad а по sz). 

32_COPY_TRAITS:
-----------

**hist7 / traits files -----( copying ) ---> ../RESULT/traits_copy** 

33_process:
-----------

**пересечение uidsz.gz за 30 дней с файлами трейтов predict_add.gz, predict_gr_v2.gz, ctr10gr.gz, ctr11gr.gz** 
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

	 мержит все uidsz за 30 дней, уникалит и подает на вход скриту 
	 out 
	 sz * all * male * fem * age1 * age2 * age3 * ctr1 * ctr2 * ctr3 * ... ctr11 * trash 

33_process_v1:
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

	 мержит все uidsz за 30 дней, уникалит и подает на вход скриту 
	 out 
	 sz * all * male * fem * age1 * age2 * age3 * ctr1 * ctr2 * ctr3 * ... ctr11 * trash 

33_TRAITS:
-----------

**uidsz [за 30 дней] + traits_copy/.. ------(33_process)--------->traits30days.gz** 

51_process:
-----------

**Объединение сумм из отдельных: bids.gz, tn0/.../total.gz, tn1/.../total.gz, sessions.gz** 
 в одну таблицу по sz + ad. 
за 30 дней 
смерженные файлы bids.gz: 
30 days bids.gz мержатся по первым двум полям (sz,ad), третье поле (bids_count) суммируется, четвертое (dom) - берется последнее значение по данному ключу: 
(ad нужна для posi_bids - ни равны 0 при ad==0) 

	 OUT: 
	 | field           | description  | 
	 | --------------- | ------------ | 
	 | sz              | 
	 | ctr             | клики/показы | 
	 | winbids         | процент выигранных нами бидов (просмотры/биды) | 
	 | avg_session     | ср длина пользов. сессии на домене             | 
	 | all_uids        | вего кук на сайтзоне                           | 
	 | avg_winprice    | средний winprice                               | 
	 | avg_exp_div_win | среднее отношение expprice/winprice            | 
	 | posibids	       | отношение posibids/bids - доля бидов с положит ставкой | 
	 | win_posi_bids   | доля выигранных бидов от тех, где ставка>0             | 
	 | pzbt_count      | количество сочетаний bt-pz                             | 
	 | 16 trait fields | 16 полей: male,female,young,adult,old,ctr1,...ctr11    | 

51_SZVARS_30DAYS:
-----------

**30 days of: tn0, tn1, bids, sessions, psbt --------( ./51_process )-------> sz_vars_30days.gz** 

95_REMOVE:
-----------

**Удаление старых данных.** 

	 Удаление часовых данных, (в основном, для tn3) (для экономии места). 

на случай если захочется взглянуть на часовые данные, удаляем только старее чем: 
hours_older="2days" 
любые файлы удаляются, которые старше (по дате в имени/пути): 
all_older="38days" 

Dom.pm:
-----------

**Перевод домена (1-параметр, строка) в домен 2-го уровня** 
Второй параметр - опции { d3l => ["regexp1","regexp2",...] } 
 попадающие под регексп - переводятся в 3-й уровень. 
Для всех производится очистка от порта, протокола, пути, пробелов 

