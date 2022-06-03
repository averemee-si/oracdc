### 'c' - INSERT message example

```
{
  "schema" : {
    "type" : "struct",
    "fields" : [ {
      "type" : "string",
      "optional" : false,
      "field" : "op"
    }, {
      "type" : "int8",
      "optional" : true,
      "field" : "ts_ms"
    }, {
      "type" : "struct",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "DEPTNO"
      } ],
      "optional" : false,
      "name" : "SCOTT.DEPT.Key",
      "version" : 1,
      "field" : "before"
    }, {
      "type" : "struct",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "DEPTNO"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "DNAME"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "LOC"
      } ],
      "optional" : true,
      "name" : "SCOTT.DEPT.Value",
      "version" : 1,
      "field" : "after"
    }, {
      "type" : "struct",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "instance_number"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "version"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "instance_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "host_name"
      }, {
        "type" : "int8",
        "optional" : false,
        "field" : "dbid"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "database_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "platform_name"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "query"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "pdb_name"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "owner"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "table"
      }, {
        "type" : "int8",
        "optional" : false,
        "field" : "scn"
      }, {
        "type" : "int8",
        "optional" : false,
        "field" : "ts_ms"
      } ],
      "optional" : false,
      "name" : "solutions.a2.cdc.oracle.Source",
      "field" : "source"
    } ],
    "optional" : false,
    "name" : "SCOTT.DEPT.Envelope"
  },
  "payload" : {
    "op" : "c",
    "ts_ms" : 1580465954805,
    "before" : {
      "DEPTNO" : 50
    },
    "after" : {
      "DEPTNO" : 50,
      "DNAME" : "MARKETING",
      "LOC" : "MIAMI"
    },
    "source" : {
      "instance_number" : 1,
      "version" : "12.1.0.2.0",
      "instance_name" : "EBSDB",
      "host_name" : "apps.example.com",
      "dbid" : 710804450,
      "database_name" : "EBSDB",
      "platform_name" : "Linux x86 64-bit",
      "query" : null,
      "pdb_name" : null,
      "owner" : "SCOTT",
      "table" : "DEPT",
      "scn" : 12204753723754,
      "ts_ms" : 1580464499942
    }
  }
}
```
### 'u' - UPDATE message example

```
{
  "schema" : {
    "type" : "struct",
    "fields" : [ {
      "type" : "string",
      "optional" : false,
      "field" : "op"
    }, {
      "type" : "int8",
      "optional" : true,
      "field" : "ts_ms"
    }, {
      "type" : "struct",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "DEPTNO"
      } ],
      "optional" : false,
      "name" : "SCOTT.DEPT.Key",
      "version" : 1,
      "field" : "before"
    }, {
      "type" : "struct",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "DEPTNO"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "DNAME"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "LOC"
      } ],
      "optional" : true,
      "name" : "SCOTT.DEPT.Value",
      "version" : 1,
      "field" : "after"
    }, {
      "type" : "struct",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "instance_number"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "version"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "instance_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "host_name"
      }, {
        "type" : "int8",
        "optional" : false,
        "field" : "dbid"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "database_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "platform_name"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "query"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "pdb_name"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "owner"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "table"
      }, {
        "type" : "int8",
        "optional" : false,
        "field" : "scn"
      }, {
        "type" : "int8",
        "optional" : false,
        "field" : "ts_ms"
      } ],
      "optional" : false,
      "name" : "solutions.a2.cdc.oracle.Source",
      "field" : "source"
    } ],
    "optional" : false,
    "name" : "SCOTT.DEPT.Envelope"
  },
  "payload" : {
    "op" : "u",
    "ts_ms" : 1580465954791,
    "before" : {
      "DEPTNO" : 10
    },
    "after" : {
      "DEPTNO" : 10,
      "DNAME" : "ACCOUNTING",
      "LOC" : "NEW YORK"
    },
    "source" : {
      "instance_number" : 1,
      "version" : "12.1.0.2.0",
      "instance_name" : "EBSDB",
      "host_name" : "apps.example.com",
      "dbid" : 710804450,
      "database_name" : "EBSDB",
      "platform_name" : "Linux x86 64-bit",
      "query" : null,
      "pdb_name" : null,
      "owner" : "SCOTT",
      "table" : "DEPT",
      "scn" : 12204753723754,
      "ts_ms" : 1580464499942
    }
  }
}
```
### 'd' - DELETE message example

```
{
  "schema" : {
    "type" : "struct",
    "fields" : [ {
      "type" : "string",
      "optional" : false,
      "field" : "op"
    }, {
      "type" : "int8",
      "optional" : true,
      "field" : "ts_ms"
    }, {
      "type" : "struct",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "DEPTNO"
      } ],
      "optional" : false,
      "name" : "SCOTT.DEPT.Key",
      "version" : 1,
      "field" : "before"
    }, {
      "type" : "struct",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "DEPTNO"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "DNAME"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "LOC"
      } ],
      "optional" : true,
      "name" : "SCOTT.DEPT.Value",
      "version" : 1,
      "field" : "after"
    }, {
      "type" : "struct",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "instance_number"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "version"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "instance_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "host_name"
      }, {
        "type" : "int8",
        "optional" : false,
        "field" : "dbid"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "database_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "platform_name"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "query"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "pdb_name"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "owner"
      }, {
        "type" : "string",
        "optional" : true,
        "field" : "table"
      }, {
        "type" : "int8",
        "optional" : false,
        "field" : "scn"
      }, {
        "type" : "int8",
        "optional" : false,
        "field" : "ts_ms"
      } ],
      "optional" : false,
      "name" : "solutions.a2.cdc.oracle.Source",
      "field" : "source"
    } ],
    "optional" : false,
    "name" : "SCOTT.DEPT.Envelope"
  },
  "payload" : {
    "op" : "d",
    "ts_ms" : 1580465954794,
    "before" : {
      "DEPTNO" : 50
    },
    "after" : null,
    "source" : {
      "instance_number" : 1,
      "version" : "12.1.0.2.0",
      "instance_name" : "EBSDB",
      "host_name" : "apps.example.com",
      "dbid" : 710804450,
      "database_name" : "EBSDB",
      "platform_name" : "Linux x86 64-bit",
      "query" : null,
      "pdb_name" : null,
      "owner" : "SCOTT",
      "table" : "DEPT",
      "scn" : 12204753723754,
      "ts_ms" : 1580464499942
    }
  }
}
```
