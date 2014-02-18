00_ABOUT:
-----------

**"Подготовка данных для кластеризации сайтзон для байесовского биддера"** 

01_START_ALL:
-----------

**Запуск всех скриптов.** 

01_STOP_ALL:
-----------

**Остановка всех скриптов.** 

10_:
-----------

**Сбор часовых данных из hl за период 30 дней.** 

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

11_:
-----------

**Удаление часовых данных из tn3 (для экономии места)** 

	 с задержкой в два дня (на случай захочется взглянуть на часовые данные). 

12_:
-----------

**Мержит часы (YYYY-MM-HH.gz) в сутки (total.gz), пропуская полученный поток через скрипт ./12_postf_${job}_tn ${typenum.}** 

	 См. скрипт './12_postf_'$job'_tn'$typenum. 

12_postf_google_tn0:
-----------

**Обработка смерженного (часы в сутки) потока для google, tn0.** 

	 OUT: 
	  1   2     3            4                 5 
	 sz  ad  cnt_exp  sum_exposureprice  sum_winprice 
	, где 
	    sz,ad - группировка 
	    cnt_exp - количество показов 
	    sum_exposureprice - сумма exposureprice 
	    sum_winprice - сумма winprice 

12_postf_google_tn1:
-----------

**Обработчик смерженного (часы в сутки) потока для google tn1.** 

	OUT: 
	  1  2      3 
	 sz ad  cnt_clicks 
	 ,где 
	   sz,ad - группировка 
	   cnt_clicks - количество кликов в группе 

12_postf_google_tn3:
-----------

**Обработчик смерженного (часы в сутки) потока для google tn3.** 

	 Добавляем колонку про сессии: СЕКУНДЫ начала сессии КУКИ на ДОМЕНЕ, 
	 и колонку "признак засчитываемого просмотра страницы" - незасчитываются повторные просмотры одной и тойже страницы с интервалом менее 10 сук. 
	 На входе поток за сутки (т.е период начало и конец которого можно считать разрывом сессии), лучше с 04-00 по 04-00. 
	 отсортрованный по КУКЕ и ВРЕМЕНИ. 
	 OUT: uid second sz pz bt exposureprice ad dom path sestart isview 
	 ,где 
	   sestart - секунды начала сессии 
	   isview - просмотр засчитан (1/0) 

12_postf_net-sixty_tn0:
-----------

**Обработка смерженного (часы в сутки) потока для google, tn0.** 

	 OUT: 
	  1   2     3            4                 5 
	 sz  ad  cnt_exp  sum_exposureprice  sum_winprice 
	, где 
	    sz,ad - группировка 
	    cnt_exp - количество показов 
	    sum_exposureprice - сумма exposureprice 
	    sum_winprice - сумма winprice 

12_postf_net-sixty_tn1:
-----------

**Обработчик смерженного (часы в сутки) потока для google tn1.** 

	OUT: 
	  1  2      3 
	 sz ad  cnt_clicks 
	 ,где 
	   sz,ad - группировка 
	   cnt_clicks - количество кликов в группе 

14_:
-----------

**total(tn3) ----( см ./14_process)---> bids(tn3)** 

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

15_:
-----------

**total.gz (tn3)-----(./15_process)--->sessions.gz** 

15_process:
-----------

**Making table "sessions" (tn3).** 

	 in: 
	  1   2     3   4  5       6        7   8   9     10      11 
	 uid second sz pz  bt exposureprice ad dom path sestart isview 
	 OUT: dom, sessions_count, views_count 

16_:
-----------

**total.gz (tn3) ----(./16_process)----> pzbt.gz** 

16_process:
-----------

**Сочетания pz+bt по sz.** 

17_:
-----------

**total.gz (tn3) ----(./16_process)----> urls.gz** 

17_process:
-----------

**urls.** 

	IN: 
	  1   2     3   4  5       6        7   8   9     10      11 
	 uid second sz pz  bt exposureprice ad dom path sestart isview 
	 sorted by UID,SECOND: 
	OUT: 
	 dom, ref, sessions_count, sessions_ends 
	  ,где 
	   sessions_count - количество сессий, где dom+url участвовал 
	   sessions_ends - количество сессий, где он был последним. 

18_:
-----------

**из total(tn3) ----> uidsz.gz ( Куки на сайтзонах. Сочетания uid+sz )** 

	OUT: 
	 uid, sz. (уникальные комбинации) 
	 сортировка по uid и sz. 

29_:
-----------

**Объединение нескольких таблиц(файлов) в одну с группировкой по sz,ad.** 

	 Печатаем по прежнему суммы, которые можно агрегировать за любой период. 
	 См. 29_process. 

29_process:
-----------

**Объединение сумм из отдельных: bids.gz, tn0/.../total.gz, tn1/.../total.gz, sessions.gz** 

	OUT: 
	 Sz, Ad, $exposures, $clicks, Bids, $exppr, $winpr, $dom_sessions, $dom_views 
	 ,где sz, ad - группировка 
	  exposures - показов 
	  clicks - кликов 
	  bids - бидов всего (кол-во бидов при exposureprice>0 можно получить вычтя из bids те биды где поле ad=0) 
	  exppr - сумма exposureprice 
	  winpr - сумма winprice 
	  dom_sessions - количество сессий на домене 
	  dom_views - засчитанных посещений страниц кукой на домене (равно сумме длин сессий, где длина - количество просмотров неуникальных страниц) 

30_:
-----------

**table29 [т.е. суммы по sz и ad] ------( суммирование по sz )------> получаем table30.gz [суммы по sz]** 

	 OUT: 
	   Sz, exposures, clicks, bids, exppr, winpr, dom_sessions, dom_views 
	   (аналогично table29 только суммы не по sz+ad а по sz). 

Dom.pm:
-----------

**Перевод домена (1-параметр, строка) в домен 2-го уровня** 

