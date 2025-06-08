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
REM |  Creating CDC USER and grants privileges
REM |
REM | USAGE
REM |
REM |  sqlplus <DBA_USER>/<DBA_PASSWORD>@<TNS_ALIAS_OR_ADDRESS> \
REM |          @cdc_user_cdb.sql \
REM |                  <CDC USER> <CDC_PASSWORD> <CONTAINER>
REM |
REM +========================================================================*+

set verify off;

WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
WHENEVER OSERROR  EXIT FAILURE ROLLBACK;

spool cdc_user_cdb.out

define cdcUserName ='&&1';
define cdcPassword ='&&2';
define cdcContainer ='&&3';

create user &cdcUserName
  identified by &cdcPassword
  default tablespace SYSAUX
  temporary tablespace TEMP
  container=&cdcContainer;

grant create session to &cdcUserName;
grant set container to &cdcUserName container=&cdcContainer;

@@redo_miner_fixed_tables_cdb &cdcUserName &cdcContainer
@@redo_miner_views_cdb &cdcUserName &cdcContainer

spool off;
exit;
