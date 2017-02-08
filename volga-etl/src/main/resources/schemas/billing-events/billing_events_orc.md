Hive DB sizes
=============

    /hive/warehouse/dajobe.db/billing_events  37247564943
	/hive/warehouse/dajobe.db/billing_events_stg      89137155496

raw orc with no DISTRIBUTED BY:

    /hive/warehouse/dajobe.db/billing_events_orc      19457344117

ORC with DISTRIBUTED BY by m_date and dt partitioning

    /hive/warehouse/dajobe.db/billing_events_orc      18355234084

and partition sizes:

	$ hadoop fs -dus /hive/warehouse/dajobe.db/billing_events_orc/*
	/hive/warehouse/dajobe.db/billing_events_orc/dt=1-12-01	4949
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-01	300019145
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-02	303123282
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-03	295384354
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-04	303974725
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-05	306683147
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-06	309840230
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-07	305444094
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-08	308550154
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-09	308224419
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-10	311557367
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-11	316838445
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-12	326833413
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-13	315752646
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-14	317651530
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-15	314718226
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-16	320039011
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-17	327236140
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-18	317074233
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-19	328819471
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-20	317914957
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-21	317866319
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-22	320578125
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-23	310685513
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-24	326491451
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-25	315846506
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-26	314199781
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-27	317265502
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-28	325481535
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-29	306356006
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-30	306755715
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-03-31	321789423
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-01	326411463
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-02	281307056
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-03	335600780
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-04	323617112
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-05	313536510
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-06	317099012
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-07	325592442
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-08	339196654
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-09	339042292
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-10	324493348
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-11	288334219
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-12	276856815
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-13	284032430
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-14	280211344
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-15	279114089
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-16	287741961
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-17	292471891
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-18	278621173
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-19	272412879
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-20	282291595
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-21	275510637
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-22	285552290
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-23	277258682
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-24	242803202
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-25	198394923
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-26	247138565
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-27	224966650
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-28	195826615
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-29	307053507
	/hive/warehouse/dajobe.db/billing_events_orc/dt=2014-04-30	305225146
	/hive/warehouse/dajobe.db/billing_events_orc/dt=3-03-06	5762
	/hive/warehouse/dajobe.db/billing_events_orc/dt=5-08-05	5762
	/hive/warehouse/dajobe.db/billing_events_orc/dt=5-10-02	4949
	/hive/warehouse/dajobe.db/billing_events_orc/dt=__HIVE_DEFAULT_PARTITION__	8502515

