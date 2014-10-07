# Setup

## Cassandra

Start your local Cassandra node. At the command line, cd to the /data directory.

    host:data helena$ cqlsh
    Connected to Test Cluster at 127.0.0.1:9042. 
    [cqlsh 5.0.1 | Cassandra 2.1.0 | CQL spec 3.2.0 | Native protocol v3]
    Use HELP for help.
    cqlsh> create keyspace if not exists isd_weather_data  with replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
    cqlsh> source 'create-timeseries.cql';
    cqlsh> source 'load-timeseries.cql';
    cqlsh> describe keyspace isd_weather_data;
    cqlsh> use isd_weather_data;
    cqlsh:isd_weather_data> select * from weather_station limit 20;
     
    
     id           | call_sign | country_code | elevation | lat    | long    | name                  | state_code
    --------------+-----------+--------------+-----------+--------+---------+-----------------------+------------
     408930:99999 |      OIZJ |           IR |         4 |  25.65 |  57.767 |                  JASK |       null
     725500:14942 |      KOMA |           US |     299.3 | 41.317 |   -95.9 | OMAHA EPPLEY AIRFIELD |         NE
     725474:99999 |      KCSQ |           US |       394 | 41.017 | -94.367 |               CRESTON |         IA
     480350:99999 |      VBLS |           BM |       749 | 22.933 |   97.75 |                LASHIO |       null
     719380:99999 |      CYCO |           CN |        22 | 67.817 | -115.15 |    COPPERMINE AIRPORT |       null
     992790:99999 |     DB279 |           US |         3 |   40.5 | -69.467 |   ENVIRONM BUOY 44008 |       null
      85120:99999 |      LPPD |           PO |        72 | 37.733 |   -25.7 |   PONTA DELGADA/NORDE |       null
     150140:99999 |      LRBM |           RO |       218 | 47.667 |  23.583 |             BAIA MARE |       null
     435330:99999 |      null |           MV |         1 |  6.733 |   73.15 |              HANIMADU |       null
     536150:99999 |      null |           CI |      1005 | 38.467 |  106.27 |       YINCHUAN (CITY) |       null
     724287:99999 |      KTDZ |           US |       189 |  41.55 | -83.467 |         METCALF FIELD |         OH
     541340:99999 |      null |           CI |       274 | 43.617 |  121.32 |                 KAILU |       null
     726395:14808 |      KOSC |           US |     188.1 |  44.45 |   -83.4 |  OSCODA WURTSMITH AFB |         MI
     913850:99999 |      null |           LN |         2 |  5.883 | -162.05 |        PALMYRA ISLAND |       null
     717690:99999 |      CWGB |           CN |         5 |  49.35 | -124.17 |     BALLENAS IL AUTO8 |       null
     681105:99999 |      FAWW |           NM |      1700 |  -23.5 |      17 |        WINDHOEK (MET) |       null
     716420:99999 |      CYLD |           CN |       447 | 47.817 |  -83.35 |            CHAPLEAU A |       null
      84170:99999 |      null |           SP |       569 | 37.783 |    -3.8 |                  JAEN |       null
     992740:99999 |     DB274 |           US |         3 |   40.8 |  -124.5 |   ENVIRONM BUOY 46022 |       null
     983220:99999 |      RPXC |           PH |       161 | 15.317 |  120.42 |   CROW VALLEY GNRY RG |       null
    
    (20 rows)
    
    cqlsh:isd_weather_data> 

Sweet!     
   
If you ever want to clear everything out and start fresh just:

    cqlsh> drop keyspace isd_weather_data;
    
    
* [Starting cqlsh](http://www.datastax.com/documentation/cql/3.1/cql/cql_using/useStartingCqlshTOC.html)
* [Basic cqlsh commands](http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/cqlshCommandsTOC.html)