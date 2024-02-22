remark
remark Copyright (c) 2018-present, A2 Rešitve d.o.o.
remark
remark Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
remark compliance with the License. You may obtain a copy of the License at
remark
remark http://www.apache.org/licenses/LICENSE-2.0
remark
remark Unless required by applicable law or agreed to in writing, software distributed under the License is
remark distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
remark the License for the specific language governing permissions and limitations under the License.
remark

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
