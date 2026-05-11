REM +-------------------------------------------------------------------------+
REM |
REM | This file is part of the oracdc project.
REM | Copyright (c) 2018-present, A2 Rešitve d.o.o.
REM | Authors: Aleksei Veremeev
REM |
REM | This program is offered under a commercial and under the AGPL license.
REM | For commercial licensing, contact us at sales@a2.solutions.
REM | For AGPL licensing, see below.
REM |
REM | AGPL licensing:
REM | This program is free software: you can redistribute it and/or modify
REM | it under the terms of the GNU Affero General Public License as published by
REM | the Free Software Foundation, either version 3 of the License, or
REM | (at your option) any later version.
REM |
REM | REM |his program is distributed in the hope that it will be useful,
REM | but WITHOUT ANY WARRANTY; without even the implied warranty of
REM | MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
REM | GNU Affero General Public License for more details.
REM |
REM | You should have received a copy of the GNU Affero General Public
REM | License along with this program; see the file GNU-AGPL-v3.0.adoc.
REM | If not, see <https://www.gnu.org/licenses/>.
REM |
REM +=======================================================================*+

set serveroutput on
declare
  l_thread number;
  l_arc_count number;
  l_first number;
  l_next number;
  l_name varchar2(513);
begin
  select THREAD#
  into l_thread
  from V$INSTANCE;
  select count(*)
  into l_arc_count
  from V$ARCHIVED_LOG
  where ARCHIVED='YES' and STANDBY_DEST='NO' and DELETED='NO' and THREAD#=l_thread;
  if (l_arc_count > 0) then
    select FIRST_CHANGE#, NEXT_CHANGE#, NAME
    into l_first, l_next, l_name
    from V$ARCHIVED_LOG
    where ARCHIVED='YES' and STANDBY_DEST='NO' and DELETED='NO' and THREAD#=l_thread and rownum=1;
  else
    select L.FIRST_CHANGE#, (select CURRENT_SCN from V$DATABASE), F.MEMBER
    into l_first, l_next, l_name
    from V$LOG L, V$LOGFILE F
    where L.STATUS = 'CURRENT' and L.GROUP# = F.GROUP# and L.THREAD#=l_thread and F.STATUS is null and rownum = 1;
  end if;
  -- Ready for LogMiner
  dbms_output.put_line('===========');
  dbms_output.put_line('Adding log file "' || l_name || '"');
  DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => l_name, OPTIONS => DBMS_LOGMNR.NEW);
  dbms_output.put_line('Starting LogMiner for SCN range from ' || l_first || ' to ' || l_next);
  DBMS_LOGMNR.START_LOGMNR(
    STARTSCN => l_first,
    ENDSCN => l_next,
    OPTIONS =>
      DBMS_LOGMNR.SKIP_CORRUPTION +
      DBMS_LOGMNR.NO_SQL_DELIMITER +
      DBMS_LOGMNR.NO_ROWID_IN_STMT);
  dbms_output.put_line('Querying V$LOGMNR_CONTENTS');
  select count(*)
  into l_arc_count
  from V$LOGMNR_CONTENTS;
  dbms_output.put_line('OK');
end;
/
