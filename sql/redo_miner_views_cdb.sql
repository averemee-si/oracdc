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
REM |  Data dictionary privileges required for Redo Miner
REM |
REM |
REM +========================================================================*+

set verify off;

WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
WHENEVER OSERROR  EXIT FAILURE ROLLBACK;

define cdcUserName ='&&1';
define cdcContainer ='&&2';


grant select on SYS.CDB_TABLES to &cdcUserName
    container=&cdcContainer;
grant select on SYS.CDB_INDEXES to &cdcUserName
    container=&cdcContainer;
grant select on SYS.CDB_IND_PARTITIONS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.CDB_TAB_COLS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.CDB_TAB_COLUMNS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.CDB_IND_COLUMNS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.CDB_OBJECTS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.CDB_LOBS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.CDB_LOG_GROUPS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.CDB_CONSTRAINTS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.CDB_CONS_COLUMNS to &cdcUserName
    container=&cdcContainer;

grant select on SYS.CDB_PDBS to &cdcUserName
    container=&cdcContainer;

grant select on SYS.DBA_TABLES to &cdcUserName
    container=&cdcContainer;
grant select on SYS.DBA_INDEXES to &cdcUserName
    container=&cdcContainer;
grant select on SYS.DBA_IND_PARTITIONS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.DBA_TAB_COLS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.DBA_TAB_COLUMNS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.DBA_IND_COLUMNS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.DBA_OBJECTS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.DBA_LOBS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.DBA_LOG_GROUPS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.DBA_CONSTRAINTS to &cdcUserName
    container=&cdcContainer;
grant select on SYS.DBA_CONS_COLUMNS to &cdcUserName
    container=&cdcContainer;

