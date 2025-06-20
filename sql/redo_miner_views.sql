REM +-------------------------------------------------------------------------+
REM |
REM | Copyright (c) 2018-present, A2 Rešitve d.o.o.
REM |
REM | Licensed under the Apache License, Version 2.0 (the "License");
REM | you may not use this file except in compliance with the License.
REM | You may obtain a copy of the License at
REM |
REM | http://www.apache.org/licenses/LICENSE-2.0
REM |
REM | Unless required by applicable law or agreed to in writing, software
REM | distributed under the License is distributed on an "AS IS" BASIS,
REM | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM | See the License for the specific language governing permissions and
REM | limitations under the License.
REM |
REM +-------------------------------------------------------------------------+
REM |
REM | DESCRIPTION
REM |  Data dictionary privileges required for Redo Miner
REM |
REM |
REM +========================================================================*+

set verify off;

WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
WHENEVER OSERROR  EXIT FAILURE ROLLBACK;

define cdcUserName ='&&1';

grant select on SYS.DBA_TABLES to &cdcUserName;
grant select on SYS.DBA_INDEXES to &cdcUserName;
grant select on SYS.DBA_IND_PARTITIONS to &cdcUserName;
grant select on SYS.DBA_TAB_COLS to &cdcUserName;
grant select on SYS.DBA_TAB_COLUMNS to &cdcUserName;
grant select on SYS.DBA_IND_COLUMNS to &cdcUserName;
grant select on SYS.DBA_OBJECTS to &cdcUserName;
grant select on SYS.DBA_LOBS to &cdcUserName;
grant select on SYS.DBA_LOG_GROUPS to &cdcUserName;
grant select on SYS.DBA_CONSTRAINTS to &cdcUserName;
grant select on SYS.DBA_CONS_COLUMNS to &cdcUserName;

