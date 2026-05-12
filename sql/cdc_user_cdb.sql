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
