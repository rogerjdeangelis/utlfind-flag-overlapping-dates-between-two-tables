    %let pgm=utlfind-flag-overlapping-dates-between-two-tables;

    Find overlapping dates between two tables and create a flag (SAS,python,R,WPS)

      Suggest you investigate solutions 6 and 7

      Seven Solutions

            1. SAS Sql ( interesting use of exist in SQL)
               Solution by https://stackoverflow.com/users/108797/chris-j
            2. Python SQL
            3. R SQL
            4. SAS Hash
               by https://stackoverflow.com/users/1249962/richard
            5. WPS Hash
            6. SAS Temp Array indexed by data
               Mark Keintz
               SIMPLE DESCRIPTION
               Given a range of dates and a single date, generate an array with
               every date in the range then check to see if the single new date is in the
               array of sequential dates. If the date is in the range then there
               is ovelap.
               mkeintz@outlook.com
            7. WPS Temp Array indexed by data
               Mark Keintz
               mkeintz@outlook.com


    github
    https://tinyurl.com/3myadx5e
    https://github.com/rogerjdeangelis/utlfind-flag-overlapping-dates-between-two-tables

    stackoverflow
    https://tinyurl.com/ypdfetsn
    https://stackoverflow.com/questions/75562601/need-help-finding-overlapping-dates-between-two-tables-and-creating-a-y-n-flag-i

    Related
    https://github.com/rogerjdeangelis?tab=repositories&q=overlap&type=&language=&sort=

    https://stackoverflow.com/users/108797/chris-j
    https://stackoverflow.com/users/1249962/richard


    Unfortunately it appears that Python and R depecate frquently so you may
    need to use slighly different package syntax.

    Seems to be some kind of aversion to SQL in python and R, but nto SAS nad WPS.

    gitnub
    https://tinyurl.com/khkhkxxm
    https://github.com/rogerjdeangelis/utl-sqlite-processing-in-python-with-added-math-and-stat-functions

    github
    https://tinyurl.com/5y76xszm
    https://github.com/rogerjdeangelis/utl-proc-summary-in-sas-R-and-python-sql

    Python sql
    https://tinyurl.com/yhm2wkbv
    https://github.com/rogerjdeangelis?tab=repositories&q=pandasql+in%3Areadme&type=&language=&sort=

    R sql
    https://tinyurl.com/5n8d36hs
    https://github.com/rogerjdeangelis?tab=repositories&q=sqldf+in%3Areadme&type=&language=&sort=

    StackOverflow
    https://tinyurl.com/5n73pkzd
    https://stackoverflow.com/questions/70980753/trouble-with-groupby-loop-in-python-pyspark

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  Up to 40 obs from last table WORK.HAVONE total obs=11 10APR2023:08:36:31                                              */
    /*                                                                                                                        */
    /*  Obs    ID    START     END                                                                                            */
    /*                                                                                                                        */
    /*    1     1    23011    23015                                                                                           */
    /*    2     1    23042    23056                                                                                           */
    /*    3     1    23065    23070                                                                                           */
    /*                                                                                                                        */
    /*    4     2    23020    23025                                                                                           */
    /*    5     2    23041    23045                                                                                           */
    /*    6     2    23049    23059                                                                                           */
    /*    8     3    23032    23035                                                                                           */
    /*    9     3    23042    23059                                                                                           */
    /*   10     4    23011    23016                                                                                           */
    /*   11     4    23020    23041                                                                                           */
    /*                                                                                                                        */
    /*  Up to 40 obs from last table WORK.HAVTWO total obs=15 10APR2023:08:37:51                                              */
    /*                                                                                                                        */
    /*  Obs    ID    LAB     DATE                                                                                             */
    /*                                                                                                                        */
    /*    1     1    CBC    23012                                                                                             */
    /*    2     1    CBC    23028                                                                                             */
    /*    3     1    CBC    23046                                                                                             */
    /*    4     1    CBC    23047                                                                                             */
    /*                                                                                                                        */
    /*    5     2    CBC    23011                                                                                             */
    /*    6     2    CBC    23020                                                                                             */
    /*    7     2    CBC    23049                                                                                             */
    /*    8     2    CBC    23063                                                                                             */
    /*    9     3    CBC    23016                                                                                             */
    /*   10     3    CBC    23029                                                                                             */
    /*   11     3    CBC    23062                                                                                             */
    /*   12     4    CBC    23021                                                                                             */
    /*   13     4    CBC    23041                                                                                             */
    /*   14     4    CBC    23052                                                                                             */
    /*   15     4    CBC    23063                                                                                             */
    /*                                                                                                                        */
    /*             _                                                                                                          */
    /*  _ __ _   _| | ___  ___                                                                                                */
    /* | `__| | | | |/ _ \/ __|                                                                                               */
    /* | |  | |_| | |  __/\__ \                                                                                               */
    /* |_|   \__,_|_|\___||___/                                                                                               */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /*                                                |  RULES                                                                */
    /*             HAVEONE                   HAVETWO  |  WANT                               FLAG                              */
    /*  =========================    Obs ============ |  ================================   ======                            */
    /*  Obs  ID    START     END         ID    DATE   |  CONDITION                                                            */
    /*                                                |                                                                       */
    /*    1   1    23011    23015     1   1   23012   |  20012 is between 23011 and 23015   1        OVERLAP                  */
    /*    2   1    23042    23056     2   1   23028   |  23028 is before 23042              0        BEFORE                   */
    /*                                                                                                                        */
    /*    2   1    23042    23056     3   1   23046   |  23046 is between 23042 and 23065   1        OVERLAP   ==> LOOK BACK  */
    /*                                                                                                                        */
    */ /***********************************************************************************************************************/

    libname sd1 "d:/sd1"; /* for r and python */

    data havOne sd1.havone;
    input id start end;
    attrib start end format=mmddyy10. informat=mmddyy10.;
    cards4;
     1 01/01/2023 01/05/2023
     1 02/01/2023 02/15/2023
     1 02/24/2023 03/01/2023
     2 01/10/2023 01/15/2023
     2 01/31/2023 02/04/2023
     2 02/08/2023 02/18/2023
     3 01/02/2023 01/08/2023
     3 01/22/2023 01/25/2023
     3 02/01/2023 02/18/2023
     4 01/01/2023 01/06/2023
     4 01/10/2023 01/31/2023
    ;;;;
    run;quit;

    data havTwo sd1.havtwo;
    attrib id length=8 lab length=$20 date format=mmddyy10. informat=mmddyy10.;
    input id lab date;
    cards4;
     1 CBC 1/2/2023
     1 CBC 1/18/2023
     1 CBC 02/05/2023
     1 CBC 02/06/2023
     2 CBC 01/01/2023
     2 CBC 01/10/2023
     2 CBC 02/8/2023
     2 CBC 02/22/2023
     3 CBC 01/06/2023
     3 CBC 01/19/2023
     3 CBC 02/21/2023
     4 CBC 01/11/2023
     4 CBC 01/31/2023
     4 CBC 02/11/2023
     4 CBC 02/22/2023
    ;;;;
    run;quit;


    /*                         _
     ___  __ _ ___   ___  __ _| |
    / __|/ _` / __| / __|/ _` | |
    \__ \ (_| \__ \ \__ \ (_| | |
    |___/\__,_|___/ |___/\__, |_|
                            |_|
    */


    proc sql;
       create
          table want as
       select
          havTwo.*
            , exists (
                select
                     start
                   , end
               from
                     havOne
                where
                      havTwo.id  = havOne.id
                  and havTwo.date between havOne.start and havOne.end
                ) as flag
       from
         havTwo;
    ;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* Up to 40 obs from last table WORK.WANT total obs=15 10APR2023:09:09:26                                                 */
    /*                                                                                                                        */
    /* Obs    ID    LAB     DATE    FLAG                                                                                      */
    /*                                                                                                                        */
    /*   1     1    CBC    23012      1                                                                                       */
    /*   2     1    CBC    23028      0                                                                                       */
    /*   3     1    CBC    23046      1                                                                                       */
    /*   4     1    CBC    23047      1                                                                                       */
    /*   5     2    CBC    23011      0                                                                                       */
    /*   6     2    CBC    23020      1                                                                                       */
    /*   7     2    CBC    23049      1                                                                                       */
    /*   8     2    CBC    23063      0                                                                                       */
    /*   9     3    CBC    23016      1                                                                                       */
    /*  10     3    CBC    23029      0                                                                                       */
    /*  11     3    CBC    23062      0                                                                                       */
    /*  12     4    CBC    23021      1                                                                                       */
    /*  13     4    CBC    23041      1                                                                                       */
    /*  14     4    CBC    23052      0                                                                                       */
    /*  15     4    CBC    23063      0                                                                                       */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*           _   _                             _
     _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
    | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
    | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
    | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
    |_|    |___/                                |_|
    */


    proc datasets lib=work kill;
    run;quit;

    %utlfkil(d:/xpt/want.xpt);

    %utl_pybegin;
    parmcards4;
    from os import path
    import pandas as pd
    import xport
    import xport.v56
    import pyreadstat
    import numpy as np
    import pandas as pd
    from pandasql import sqldf
    mysql = lambda q: sqldf(q, globals())
    from pandasql import PandaSQL
    pdsql = PandaSQL(persist=True)
    sqlite3conn = next(pdsql.conn.gen).connection.connection
    sqlite3conn.enable_load_extension(True)
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
    mysql = lambda q: sqldf(q, globals())
    havone, meta = pyreadstat.read_sas7bdat("d:/sd1/havone.sas7bdat")
    havtwo, meta = pyreadstat.read_sas7bdat("d:/sd1/havtwo.sas7bdat")
    print(havone);
    res = pdsql("""
          select
             havtwo.*
               , exists (
                   select
                        START
                      , END
                  from
                        havone
                   where
                         havtwo.ID  = havone.ID
                     and havtwo.DATE between havone.START and havone.END
                   ) as flag
          from
            havtwo
        ;""")
    print(res);
    ds = pyreadstat.write_xport(res,dst_path='d:/xpt/want.xpt',table_name='WANT', file_format_version=5) ;
    ;;;;
    %utl_pyend;

    libname pyxpt xport "d:/xpt/want.xpt";

    proc contents data=pyxpt._all_;
    run;quit;

    data want;
      set pyxpt.want;
      daten=input(date,yymmdd10.);
    run;quit;

    */ /*************************************************************************************************************************
    */ /*
    */ /* Obs    ID    LAB       DATE       FLAG    DATEN
    */ /*
    */ /*   1     1    CBC    2023-01-02      1     23012
    */ /*   2     1    CBC    2023-01-18      0     23028
    */ /*   3     1    CBC    2023-02-05      1     23046
    */ /*   4     1    CBC    2023-02-06      1     23047
    */ /*   5     2    CBC    2023-01-01      0     23011
    */ /*   6     2    CBC    2023-01-10      1     23020
    */ /*   7     2    CBC    2023-02-08      1     23049
    */ /*   8     2    CBC    2023-02-22      0     23063
    */ /*   9     3    CBC    2023-01-06      1     23016
    */ /*  10     3    CBC    2023-01-19      0     23029
    */ /*  11     3    CBC    2023-02-21      0     23062
    */ /*  12     4    CBC    2023-01-11      1     23021
    */ /*  13     4    CBC    2023-01-31      1     23041
    */ /*  14     4    CBC    2023-02-11      0     23052
    */ /*  15     4    CBC    2023-02-22      0     23063
    */ /*
    */ /*************************************************************************************************************************
    proc print data=want;
    run;quit;

    /*___              _
    |  _ \   ___  __ _| |
    | |_) | / __|/ _` | |
    |  _ <  \__ \ (_| | |
    |_| \_\ |___/\__, |_|
                    |_|
    */

    %utl_submit_r64("
      library(haven);
      library(SASxport);
      library(sqldf);
      havone<-read_sas('d:/sd1/havone.sas7bdat');
      havtwo<-read_sas('d:/sd1/havtwo.sas7bdat');
      havone;
      want_r<-sqldf('
          select
             havtwo.*
               , exists (
                   select
                        START
                      , END
                  from
                        havone
                   where
                         havtwo.ID  = havone.ID
                     and havtwo.DATE between havone.START and havone.END
                   ) as flag
          from
            havtwo
      ');
      want_r;
      write.xport(want_r,file='d:/xpt/want_r.xpt');
    ");

    libname xpt xport 'd:/xpt/want_r.xpt';

    proc print data=xpt.want_r;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*    Obs    ID    LAB     DATE    FLAG                                                                                   */
    /*                                                                                                                        */
    /*      1     1    CBC    23012      1                                                                                    */
    /*      2     1    CBC    23028      0                                                                                    */
    /*      3     1    CBC    23046      1                                                                                    */
    /*      4     1    CBC    23047      1                                                                                    */
    /*      5     2    CBC    23011      0                                                                                    */
    /*      6     2    CBC    23020      1                                                                                    */
    /*      7     2    CBC    23049      1                                                                                    */
    /*      8     2    CBC    23063      0                                                                                    */
    /*      9     3    CBC    23016      1                                                                                    */
    /*     10     3    CBC    23029      0                                                                                    */
    /*     11     3    CBC    23062      0                                                                                    */
    /*     12     4    CBC    23021      1                                                                                    */
    /*     13     4    CBC    23041      1                                                                                    */
    /*     14     4    CBC    23052      0                                                                                    */
    /*     15     4    CBC    23063      0                                                                                    */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*                              _
    __      ___ __  ___   ___  __ _| |
    \ \ /\ / / `_ \/ __| / __|/ _` | |
     \ V  V /| |_) \__ \ \__ \ (_| | |
      \_/\_/ | .__/|___/ |___/\__, |_|
             |_|                 |_|
    */

    %utl_submit_wps64('

    options validvarname=any;

    libname sd1 "d:/sd1";

    proc sql;
       create
            table want_wps as
       select
            havTwo.*
            , exists (
                select
                     start
                   , end
               from
                     sd1.havOne
                where
                      havTwo.id  = havOne.id
                  and havTwo.date between havOne.start and havOne.end
                ) as flag
       from
         sd1.havTwo;
    ;quit;

    proc contents data=want_wps;
    run;quit;

    proc print data=want_wps;
    run;quit;

    ')

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*                        Alphabetic List of Variables and Attributes                                                     */
    /*                                                                                                                        */
    /*       Number    Variable    Type             Len             Pos    Format       Informat                              */
    /* __________________________________________________________________________________________                             */
    /*            3    DATE        Num                8               8    MMDDYY10.    MMDDYY10.                             */
    /*            1    ID          Num                8               0                                                       */
    /*            2    LAB         Char              20              24                                                       */
    /*            4    flag        Num                8              16                                                       */
    /*                                                                                                                        */
    /* Obs    ID    LAB          DATE    flag                                                                                 */
    /*                                                                                                                        */
    /*   1     1    CBC    01/02/2023      1                                                                                  */
    /*   2     1    CBC    01/18/2023      0                                                                                  */
    /*   3     1    CBC    02/05/2023      1                                                                                  */
    /*   4     1    CBC    02/06/2023      1                                                                                  */
    /*   5     2    CBC    01/01/2023      0                                                                                  */
    /*   6     2    CBC    01/10/2023      1                                                                                  */
    /*   7     2    CBC    02/08/2023      1                                                                                  */
    /*   8     2    CBC    02/22/2023      0                                                                                  */
    /*   9     3    CBC    01/06/2023      1                                                                                  */
    /*  10     3    CBC    01/19/2023      0                                                                                  */
    /*  11     3    CBC    02/21/2023      0                                                                                  */
    /*  12     4    CBC    01/11/2023      1                                                                                  */
    /*  13     4    CBC    01/31/2023      1                                                                                  */
    /*  14     4    CBC    02/11/2023      0                                                                                  */
    /*  15     4    CBC    02/22/2023      0                                                                                  */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*               _               _
     ___  __ _ ___  | |__   __ _ ___| |__
    / __|/ _` / __| | `_ \ / _` / __| `_ \
    \__ \ (_| \__ \ | | | | (_| \__ \ | | |
    |___/\__,_|___/ |_| |_|\__,_|___/_| |_|

    */

    data adho;
      set sd1. havOne;
      usubjid  = id;
      hsp_strt = start;
      hsp_end   = end;
      keep
        usubjid
        hsp_strt
        hsp_end
      ;
    run;quit;


    data adlb;
     set sd1.havTwo;
      usubjid  = id;
      date_lab = date;
      keep
        usubjid
        date_lab
      ;
    run;quit;

    data want_sas_hash ;
      set adlb ;
      /* initialise the hash table */
      if 0 then set adho ;
      if _n_ = 1 then do ;
        declare hash h(dataset:'adho',multidata:'y') ;
        h.definekey('usubjid') ;
        h.definedata('hsp_strt','hsp_end');
        h.definedone() ;
      end ;

      rc = h.reset_dup() ; /* reset the hash pointer when using multidata:y and do_over() */
      flag = 0 ;
      /* do_over will loop over all matching usubjid's in the hash table */
      do while (h.do_over() = 0 and flag = 0) ;
        flag = hsp_strt <= date_lab <= hsp_end ;
      end ;

      drop rc ;
    run ;


    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* Up to 40 obs from last table WORK.WANT_SAS_HASH total obs=15 10APR2023:10:48:48                                        */
    /*                                                                                                                        */
    /* Obs    USUBJID    DATE_LAB    HSP_STRT    HSP_END    FLAG                                                              */
    /*                                                                                                                        */
    /*   1       1         23012       23042      23056       1                                                               */
    /*   2       1         23028       23065      23070       0                                                               */
    /*   3       1         23046       23065      23070       1                                                               */
    /*   4       1         23047       23065      23070       1                                                               */
    /*   5       2         23011       23049      23059       0                                                               */
    /*   6       2         23020       23041      23045       1                                                               */
    /*   7       2         23049       23049      23059       1                                                               */
    /*   8       2         23063       23049      23059       0                                                               */
    /*   9       3         23016       23032      23035       1                                                               */
    /*  10       3         23029       23042      23059       0                                                               */
    /*  11       3         23062       23042      23059       0                                                               */
    /*  12       4         23021       23020      23041       1                                                               */
    /*  13       4         23041       23020      23041       1                                                               */
    /*  14       4         23052       23020      23041       0                                                               */
    /*  15       4         23063       23020      23041       0                                                               */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*                    _               _
    __      ___ __  ___  | |__   __ _ ___| |__
    \ \ /\ / / `_ \/ __| | `_ \ / _` / __| `_ \
     \ V  V /| |_) \__ \ | | | | (_| \__ \ | | |
      \_/\_/ | .__/|___/ |_| |_|\__,_|___/_| |_|
             |_|
    */

    data sd1.adho;
      set sd1.havOne;
      usubjid  = id;
      hsp_strt = start;
      hsp_end   = end;
      keep
        usubjid
        hsp_strt
        hsp_end
      ;
    run;quit;


    data sd1.adlb;
     set sd1.havTwo;
      usubjid  = id;
      date_lab = date;
      keep
        usubjid
        date_lab
      ;
    run;quit;

    %utl_submit_wps64("

    libname sd1 'd:/sd1';

    data want_sas_hash ;
      set sd1.adlb ;
      /* initialise the hash table */
      if 0 then set sd1.adho ;
      if _n_ = 1 then do ;
        declare hash h(dataset:'sd1.adho',multidata:'y') ;
        h.definekey('usubjid') ;
        h.definedata('hsp_strt','hsp_end');
        h.definedone() ;
      end ;

      flag = 0 ;
      /* do_over will loop over all matching usubjid's in the hash table */
      set sd1.adho;
      do rc=h.find() by 0 while (rc=0);
         flag = hsp_strt <= date_lab <= hsp_end ;
          output;
          rc=h.find_next();
      end ;

      drop rc ;
    run ;
    ");

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* Up to 40 obs from WANT_SAS_HASH total obs=15 10APR2023:11:10:01                                                        */
    /*                                                                                                                        */
    /* Obs    USUBJID    DATE_LAB    HSP_STRT    HSP_END    FLAG                                                              */
    /*                                                                                                                        */
    /*   1       1         23012       23042      23056       1                                                               */
    /*   2       1         23028       23065      23070       0                                                               */
    /*   3       1         23046       23065      23070       1                                                               */
    /*   4       1         23047       23065      23070       1                                                               */
    /*   5       2         23011       23049      23059       0                                                               */
    /*   6       2         23020       23041      23045       1                                                               */
    /*   7       2         23049       23049      23059       1                                                               */
    /*   8       2         23063       23049      23059       0                                                               */
    /*   9       3         23016       23032      23035       1                                                               */
    /*  10       3         23029       23042      23059       0                                                               */
    /*  11       3         23062       23042      23059       0                                                               */
    /*  12       4         23021       23020      23041       1                                                               */
    /*  13       4         23041       23020      23041       1                                                               */
    /*  14       4         23052       23020      23041       0                                                               */
    /*  15       4         23063       23020      23041       0                                                               */
    /*                                                                                                                        */
    /**************************************************************************************************************************/


    /*__      _
     / /_    | |_ ___ _ __ ___  _ __     __ _ _ __ _ __ __ _ _   _
    | `_ \   | __/ _ \ `_ ` _ \| `_ \   / _` | `__| `__/ _` | | | |
    | (_) |  | ||  __/ | | | | | |_) | | (_| | |  | | | (_| | |_| |
     \___(_)  \__\___|_| |_| |_| .__/   \__,_|_|  |_|  \__,_|\__, |
                               |_|                           |___/
    */


    /*
      _____            _       _
     | ____|_  ___ __ | | __ _(_)_ __
     |  _| \ \/ / `_ \| |/ _` | | `_ \
     | |___ >  <| |_) | | (_| | | | | |
     |_____/_/\_\ .__/|_|\__,_|_|_| |_|
                |_|
    */

    If the data are sorted into groups as in your example, I wouldn't bother with a hash.
    Instead make a _temporary_ array indexed by date.  Key-indexing by date is always at
    least as fast as hash lookup.  The only "downside" -- make sure the macrovars
    HISTORY_BEG and HISTORY_END enclose all expected START/END ranges:

    Simple description
    Given a range of dates and a single date, generate an array with
    every date in the range then check to see if the single new date is in the
    array of sequential dates. If the date is in the range then there
    is ovelap.

    I will explain using just one observation from havOne and HavTwo.
    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */

    libname sd1 "d:/sd1"; /* for r and python */

    data havOne sd1.havone;
    input id start end;
    attrib start end format=mmddyy10. informat=mmddyy10.;
    cards4;
     1 01/01/2023 01/05/2023
    ;;;;
    run;quit;

    data havTwo sd1.havtwo;
    attrib id length=8 lab length=$20 date format=mmddyy10. informat=mmddyy10.;
    input id lab date;
    cards4;
     1 CBC 1/5/2023
    ;;;;
    run;quit;

    /*---- Must represent the maximum range in havOne and havTwo         ----*/
    /*---- Slugging a logical True into a matching date slot of an array ----*/

    %let history_beg=01jan2023;
    %let history_end=06jan2023;

    /******************************************************************************************/
    /*                                                                                        */
    /* %let history_beg=01jan2023;  max range                                                 */
    /* %let history_end=06jan2023;                                                            */
    /*                                                                                        */
    /*  HAVONE with formatted variables  total obs=1 12APR2023:12:19:59                       */
    /*  Obs    ID      START          END                                                     */
    /*                                                                                        */
    /*   1      1    01/01/2023    01/05/2023                                                 */
    /*                                                                                        */
    /*  HAVTWO with formatted variables  total obs=1 12APR2023:12:20:34                       */
    /*  Obs    ID    LAB       DATE                                                           */
    /*                                                                                        */
    /*   1      1    CBC    01/06/2023                                                        */
    /*                                                                                        */
    /******************************************************************************************/
    /*         _       _   _
     ___  ___ | |_   _| |_(_) ___  _ __
    / __|/ _ \| | | | | __| |/ _ \| `_ \
    \__ \ (_) | | |_| | |_| | (_) | | | |
    |___/\___/|_|\__,_|\__|_|\___/|_| |_|

    */

    %let history_beg=01JAN2023;
    %let history_end=06JAN2023;

    data want (drop=_: start end val txt idx);

      set havone (in=inone)    havtwo (in=intwo) ;
      by id;

      /*---- Creates an array of with dimension large enough to cover all possible dates       ----*/
      /*---- in both havOne and HavTwo.                                                        ----*/
      /*---- The index values will be '01JAN23011'd to '06JAN2023'd or 32011-23016             ----*/

      array history {%sysevalf("&history_beg"d):%sysevalf("&history_end"d)}  _temporary_ ;

      /*---- lets display the contents of the array                                            ----*/
      do idx="&history_beg"d to "&history_end"d;
         val=history{idx};
         txt=catx(" ","history{",put(idx,date9.),"} =",val, put(idx,8.));
         put txt;
      end;

      /*********************************************************************************************/
      /*                                                                                           */
      /* Array values  (all missing)                                                               */
      /*                                                                                           */
      /* history{ 23011 01JAN2023 } = .                                                            */
      /* history{ 23012 02JAN2023 } = .                                                            */
      /* history{ 23013 03JAN2023 } = .                                                            */
      /* history{ 23014 04JAN2023 } = .                                                            */
      /* history{ 23015 05JAN2023 } = .                                                            */
      /* history{ 23016 06JAN2023 } = .                                                            */
      /*                                                                                           */
      /*********************************************************************************************/

      /*---- Since temporary array vales are retaibed we must reset on each consecutive record ----*/
      if first.id then call missing(of history{*});

      /* load the array elements with 1 if date is present in the range of dates in the array  ----*/
      if inone then do _d=start to end;
        history{_d}=1;
      end;

      /*---- lets display the contents of the array                                            ----*/
      do idx="&history_beg"d to "&history_end"d;
         val=history{idx};
         txt=catx(" ","history{",put(idx,date9.),"} =",val, put(idx,8.));
         put txt;
      end;

      /*********************************************************************************************/
      /*                                                                                           */
      /* Note history{ 23016  06JAN2023 } is missing because it is not in the start to end range   */
      /* If havTwo date was 06JAN2023 then we would not have had a 1 and thus no overlap           */
      /*                                                                                           */
      /* history{ 23011  01JAN2023 } = 1 If havTwo date is any of these then set flag to 1         */
      /* history{ 23012  02JAN2023 } = 1                                                           */
      /* history{ 23013  03JAN2023 } = 1                                                           */
      /* history{ 23014  04JAN2023 } = 1                                                           */
      /* history{ 23015  05JAN2023 } = 1                                                           */
      /* history{ 23016  06JAN2023 } = .  Not considered                                           */
      /*********************************************************************************************/

      if intwo;

      /*---- if the date is in the start to end range then we have overlap                     ----*/
      flag=(history{date}=1);

    run;
    /*           _               _
      ___  _   _| |_ _ __  _   _| |_
     / _ \| | | | __| `_ \| | | | __|
    | (_) | |_| | |_| |_) | |_| | |_
     \___/ \__,_|\__| .__/ \__,_|\__|
                    |_|
    */

    /*___      _____      _ _             _       _   _
    |___ \    |  ___|   _| | |  ___  ___ | |_   _| |_(_) ___  _ __
      __) |   | |_ | | | | | | / __|/ _ \| | | | | __| |/ _ \| `_ \
     / __/ _  |  _|| |_| | | | \__ \ (_) | | |_| | |_| | (_) | | | |
    |_____(_) |_|   \__,_|_|_| |___/\___/|_|\__,_|\__|_|\___/|_| |_|

    */

     /**********************************************************************************************/
     /*                                                                                            */
     /* WANT with formatted variables  total obs=1 12APR2023:13:14:55                              */
     /*                                                                                            */
     /* Obs    ID    LAB       DATE       FLAG                                                     */
     /*                                                                                            */
     /*  1      1    CBC    01/05/2023      1                                                      */
     /*                                                                                            */
     /**********************************************************************************************/

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */

    libname sd1 "d:/sd1"; /* for r and python */

    data havOne sd1.havone;
    input id start end;
    attrib start end format=mmddyy10. informat=mmddyy10.;
    cards4;
     1 01/01/2023 01/05/2023
     1 02/01/2023 02/15/2023
     1 02/24/2023 03/01/2023
     2 01/10/2023 01/15/2023
     2 01/31/2023 02/04/2023
     2 02/08/2023 02/18/2023
     3 01/02/2023 01/08/2023
     3 01/22/2023 01/25/2023
     3 02/01/2023 02/18/2023
     4 01/01/2023 01/06/2023
     4 01/10/2023 01/31/2023
    ;;;;
    run;quit;

    data havTwo sd1.havtwo;
    attrib id length=8 lab length=$20 date format=mmddyy10. informat=mmddyy10.;
    input id lab date;
    cards4;
     1 CBC 1/2/2023
     1 CBC 1/18/2023
     1 CBC 02/05/2023
     1 CBC 02/06/2023
     2 CBC 01/01/2023
     2 CBC 01/10/2023
     2 CBC 02/8/2023
     2 CBC 02/22/2023
     3 CBC 01/06/2023
     3 CBC 01/19/2023
     3 CBC 02/21/2023
     4 CBC 01/11/2023
     4 CBC 01/31/2023
     4 CBC 02/11/2023
     4 CBC 02/22/2023
    ;;;;
    run;quit;

    /*         _       _   _
     ___  ___ | |_   _| |_(_) ___  _ __
    / __|/ _ \| | | | | __| |/ _ \| `_ \
    \__ \ (_) | | |_| | |_| | (_) | | | |
    |___/\___/|_|\__,_|\__|_|\___/|_| |_|

    */

    %let history_beg=01jan2023;
    %let history_end=15apr2023;

    data want (drop=_: start end);

      set havone (in=inone)    havtwo (in=intwo) ;
      by id;

      array history {%sysevalf("&history_beg"d):%sysevalf("&history_end"d)}  _temporary_;

      if first.id then call missing(of history{*});

      if inone then do _d=start to end;
        history{_d}=1;
      end;

      if intwo;
      flag=(history{date}=1);

    run;

    /*           _               _
      ___  _   _| |_ _ __  _   _| |_
     / _ \| | | | __| `_ \| | | | __|
    | (_) | |_| | |_| |_) | |_| | |_
     \___/ \__,_|\__| .__/ \__,_|\__|
                    |_|
    */
     /**********************************************************************************************/
     /*                                                                                            */
     /* Up to 40 obs from WANT with formatted variables  total obs=15 12APR2023:13:48:51           */                                                                          */
     /*                                                                                            */
     /* Obs    ID    LAB       DATE       FLAG                                                     */                                                                          */
     /*                                                                                            */
     /*   1     1    CBC    01/02/2023      1                                                      */                                                                                 */
     /*   2     1    CBC    01/18/2023      0                                                      */                                                                                 */
     /*   3     1    CBC    02/05/2023      1                                                      */                                                                                 */
     /*   4     1    CBC    02/06/2023      1                                                      */                                                                                 */
     /*   5     2    CBC    01/01/2023      0                                                      */                                                                                 */
     /*   6     2    CBC    01/10/2023      1                                                      */                                                                                 */
     /*   7     2    CBC    02/08/2023      1                                                      */                                                                                 */
     /*   8     2    CBC    02/22/2023      0                                                      */                                                                                 */
     /*   9     3    CBC    01/06/2023      1                                                      */                                                                                 */
     /*  10     3    CBC    01/19/2023      0                                                      */                                                                                 */
     /*  11     3    CBC    02/21/2023      0                                                      */                                                                                 */
     /*  12     4    CBC    01/11/2023      1                                                      */                                                                                 */
     /*  13     4    CBC    01/31/2023      1                                                      */                                                                                 */
     /*  14     4    CBC    02/11/2023      0                                                      */                                                                                 */
     /*  15     4    CBC    02/22/2023      0                                                      */                                                                                 */
     /*                                                                                            */
     /**********************************************************************************************/

    /*____                        _
    |___  | __      ___ __  ___  | |_ ___ _ __ ___  _ __     __ _ _ __ _ __ __ _ _   _
       / /  \ \ /\ / / `_ \/ __| | __/ _ \ `_ ` _ \| `_ \   / _` | `__| `__/ _` | | | |
      / /    \ V  V /| |_) \__ \ | ||  __/ | | | | | |_) | | (_| | |  | | | (_| | |_| |
     /_/      \_/\_/ | .__/|___/  \__\___|_| |_| |_| .__/   \__,_|_|  |_|  \__,_|\__, |
                     |_|                           |_|                           |___/
    */

    %utl_submit_wps64('

    libname sd1 "d:/sd1";

    %let history_beg=01jan2023;
    %let history_end=15apr2023;

    data want (drop=_: start end);

      set sd1.havone (in=inone) sd1.havtwo (in=intwo) ;
      by id;

      array history {%sysevalf("&history_beg"d):%sysevalf("&history_end"d)}  _temporary_;

      if first.id then call missing(of history{*});

      if inone then do _d=start to end;
        history{_d}=1;
      end;

      if intwo;
      flag=(history{date}=1);

    run;

    proc print;
    run;quit;
    ');

     /**********************************************************************************************/
     /*                                                                                            */
     /* The WPS System                                                                             */
     /*                                                                                            */
     /* Obs    ID    LAB          DATE    FLAG                                                     */
     /*                                                                                            */
     /*   1     1    CBC    01/02/2023      1                                                      */
     /*   2     1    CBC    01/18/2023      0                                                      */
     /*   3     1    CBC    02/05/2023      1                                                      */
     /*   4     1    CBC    02/06/2023      1                                                      */
     /*   5     2    CBC    01/01/2023      0                                                      */
     /*   6     2    CBC    01/10/2023      1                                                      */
     /*   7     2    CBC    02/08/2023      1                                                      */
     /*   8     2    CBC    02/22/2023      0                                                      */
     /*   9     3    CBC    01/06/2023      1                                                      */
     /*  10     3    CBC    01/19/2023      0                                                      */
     /*  11     3    CBC    02/21/2023      0                                                      */
     /*  12     4    CBC    01/11/2023      1                                                      */
     /*  13     4    CBC    01/31/2023      1                                                      */
     /*  14     4    CBC    02/11/2023      0                                                      */
     /*  15     4    CBC    02/22/2023      0                                                      */
     /*                                                                                            */
     /**********************************************************************************************/


    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */



