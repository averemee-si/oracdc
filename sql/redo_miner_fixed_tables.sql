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
REM |  Fixed tables privileges required for Redo Miner
REM |
REM |
REM +========================================================================*+

set verify off;

WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
WHENEVER OSERROR  EXIT FAILURE ROLLBACK;

define cdcUserName ='&&1';

grant select on SYS.V_$DATABASE to &cdcUserName;
grant select on SYS.V_$INSTANCE to &cdcUserName;
grant select on SYS.V_$ACTIVE_INSTANCES to &cdcUserName;
grant select on SYS.V_$THREAD to &cdcUserName;
grant select on SYS.V_$ARCHIVED_LOG to &cdcUserName;
grant select on SYS.V_$LOG to &cdcUserName;
grant select on SYS.V_$LOGFILE to &cdcUserName;
grant select on SYS.V_$TRANSPORTABLE_PLATFORM to &cdcUserName;
grant select on SYS.V_$NLS_PARAMETERS to &cdcUserName;
grant select on SYS.V_$STANDBY_LOG to &cdcUserName;
grant select on SYS.V_$LICENSE to &cdcUserName;
grant select on SYS.GV_$INSTANCE to &cdcUserName;

