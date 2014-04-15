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

11_REMOVE:
-----------

**Удаление старых данных.** 
параметр 
job=${1:? Job! } 
параметр: 
typenum=${2:? Typenum! } 
Удаление часовых данных, (в основном, для tn3) (для экономии места). 
на случай если захочется взглянуть на часовые данные, удаляем только старее чем: 
hours_older="2days" 
Удаление любых данных /RESULT/10/2014-01-18/$job/$typenum, старее указанного: 
all_older="45days" 

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

13_REMOVE:
-----------

**Удаление пустых файлов.** 
параметр: 
job=${1:? Job! } 
параметр: 
typenum=${2:? Typenum! } 
Удаление пустых данных ВСЕХ $job, когда gz-файлы ==20байт 

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
	 ,где 
	   sz, ad - группировка 
	   bids - количество бидов по группе. 
	     Прим. 
	      Поле "количество бидов при exposureprice>0" (posibids) отсутствует, потому что 
	      оно равно значению поля bids, за исключением случаев, когда ad=0. В этом случае оно равно 0. 
	   domain - домен (как внешний ключ для объединения с данными, сгруппированными по доменам ) 

15_process:
-----------

**Making table "sessions" (tn3).** 

	 in: 
	  1   2     3   4  5       6        7   8   9     10      11 
	 uid second sz pz  bt exposureprice ad dom path sestart isview 
	 OUT: dom, sessions_count, views_count 

15_SESSIONS:
-----------

**total.gz (tn3)-----(./15_process)--->sessions.gz** 
параметр: 
job=${1:?JOB!} 

16_process:
-----------

**Сочетания pz+bt по sz.** 
 1   2     3   4  5       6        7   8   9     10      11 
uid second sz pz  bt exposureprice ad dom path sestart isview 

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
	  ,где 
	   sessions_count - количество сессий, где dom+url участвовал 
	   sessions_ends - количество сессий, где он был последним. 

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

19_URLS30DAYS:
-----------

**urls.gz -----(19_process)---------->urls30days.gz** 

20_DOM30DAYS:
-----------

**urls30days.gz -----(cut)---------->dom30days.gz** 

21_DOMGR:
-----------

**doms30days.gz -----( ...crawler job via ssh ...)---->dom_gr.gz , quant.gz** 
Запускать 1 поток! 

21_process:
-----------

**** 

22_process:
-----------

**urls + groups = urls_group** 
Проходит по файлу urls30days (смерженные урлы за 30 дней), глядя в группы от Димы, 
 для каждого урла печатает dom*path*RV, где RV = group * 10 + subgroup 
если в quant нет такой группы, то подгруппа=0 

22_URLSGROUPS:
-----------

**Dima's domain groups ------(./21_process)------->urs_groups.gz** 

29_JOIN:
-----------

**$job/3/total + bids.gz + sessions.gz ------( 29_process )------> table29.gz** 
Объединение нескольких таблиц(файлов) в одну с группировкой по sz,ad. 

	 Печатаем по прежнему суммы, которые можно агрегировать за любой период. 
	 См. 29_process. 

29_process:
-----------

**Объединение сумм из отдельных: bids.gz, tn0/.../total.gz, tn1/.../total.gz, sessions.gz** 
 в одну таблицу по sz + ad. 
Печатаем по-прежнему только суммы, которые можно агрегировать за период. 

	OUT: 
	 
	 Sz, Ad, $exposures, $clicks, Bids, $exppr, $winpr, Dom, $dom_sessions, $dom_views 
	 | field | description | 
	 | ----- | ----------- | 
	 | sz | 
	 | ad | 
	 | exposures | показов | 
	 | clicks | кликов | 
	 | bids   | бидов всего (кол-во бидов при exposureprice>0 можно получить вычтя из bids те биды где поле ad=0) | 
	 | exppr  | сумма exposureprice | 
	 | winpr  | сумма winprice | 
	 | dom | домен, в который входит эта сайтзона (один домен - несколько сайтзон) | 
	 | dom_sessions | количество сессий на домене | 
	 | dom_views | засчитанных посещений страниц кукой на домене (равно сумме длин сессий, где длина - количество просмотров неуникальных страниц) | 

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

32_process:
-----------

**Объединение смерженных за {deep} дней uidsz с gender, age, ctr_&_inactive** 

	 gender: 
	  11 - 2.54 муж 
	  12 - 2.55 жен 
	 age: 
	  1 - 2.56 young (12-24) 
	  2 - 2.57 adult (25-44) 
	  3 - 2.60 old (45+) 
	 ctr: 1(most active)..10 groups + 11=inactive 
	 Дается имя текущего uidsz.gz 
	 Дается job (напр. google) 
	 Задается глубина дней, за сколько мержить uidsz 
	 Указывается основная папка файлов трейтов. 
	 по имени текущего uidsz получает текущий день 
	 по текущему дню получает имя соответствующего predict_add файла из трейтов по gender и проверяет его существование: 
	 тоже для age: 
	 тоже для ctr-групп: 
	 по текущему дню и {deep}, вычисляет за какие дни мержить 
	 deep дней, включая day: 
	 по дням вычисляет соответствующие файлы uidsz.gz: 
	 должны все присутствовать и быть >20b: 
	 мержит все uidsz, уникалит и подает на вход скриту 
	 на входе отуникаленные uid sz, отсортированный по uid 
	 OUT: 
	  uid sz gendered_group age_group 

32_TRAITS:
-----------

**uidsz.gz (за {deep} дней ) + gender predicted_add ---------( ./32_process )-------> gender.gz** 

	 Для каждого tn3 uidsz.gz: мержит за несколько (deep) дней такие же uidsz, включая текущий и объеденяет с gender predicted_add 
	 см ./32_process 

? Здесь мешает неопределенность, на каких серверах будут файлы с трейтами.Нужен способ указывать это гибко. 
? Мешает не всегда имеющийся полный набор файлов с трейтами, ( нужно чтоб работало и без одного или нет? ) 

только те, для которых есть прошлые данные на глубину $deep: 

Dom.pm:
-----------

**Перевод домена (1-параметр, строка) в домен 2-го уровня** 
Второй параметр - опции { d3l => ["regexp1","regexp2",...] } 
 попадающие под регексп - переводятся в 3-й уровень. 
Для всех производится очистка от порта, протокола, пути, пробелов 

