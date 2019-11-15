### 'c' - INSERT message example

```
{
  "schema" : {
    "type" : "struct",
    "optional" : false,
    "name" : "SCOTT.DEPT.Envelope",
    "fields" : [ {
      "type" : "struct",
      "optional" : true,
      "field" : "before",
      "name" : "SCOTT.DEPT.PK",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "DEPTNO"
      } ]
    }, {
      "type" : "struct",
      "optional" : true,
      "field" : "after",
      "name" : "SCOTT.DEPT.Data",
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
      } ]
    }, {
      "type" : "struct",
      "optional" : false,
      "field" : "source",
      "name" : "eu.solutions.a2.cdc.oracle",
      "fields" : [ {
        "type" : "int64",
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
        "type" : "int16",
        "optional" : false,
        "field" : "instance_number"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "instance_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "host_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "version"
      }, {
        "type" : "int64",
        "optional" : false,
        "field" : "ts_ms"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "owner"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "table"
      }, {
        "type" : "int64",
        "optional" : false,
        "field" : "scn"
      } ]
    }, {
      "type" : "string",
      "optional" : false,
      "field" : "op"
    }, {
      "type" : "int64",
      "optional" : false,
      "field" : "ts_ms"
    } ]
  },
  "payload" : {
    "before" : null,
    "after" : {
      "DEPTNO" : 10,
      "DNAME" : "ACCOUNTING",
      "LOC" : "NEW YORK"
    },
    "source" : {
      "ts_ms" : 1569178592502,
      "owner" : "SCOTT",
      "table" : "DEPT",
      "scn" : 2288632,
      "dbid" : 3346093197,
      "database_name" : "JDK8",
      "platform_name" : "Linux x86 64-bit",
      "instance_number" : 1,
      "instance_name" : "JDK8",
      "host_name" : "kafka.wine-gu.ru",
      "version" : "12.2.0.1.0"
    },
    "op" : "c",
    "ts_ms" : 1569178592503
  }
}
```
### 'u' - UPDATE message example

```
{
  "schema" : {
    "type" : "struct",
    "optional" : false,
    "name" : "SCOTT.DEPT.Envelope",
    "fields" : [ {
      "type" : "struct",
      "optional" : true,
      "field" : "before",
      "name" : "SCOTT.DEPT.PK",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "DEPTNO"
      } ]
    }, {
      "type" : "struct",
      "optional" : true,
      "field" : "after",
      "name" : "SCOTT.DEPT.Data",
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
      } ]
    }, {
      "type" : "struct",
      "optional" : false,
      "field" : "source",
      "name" : "eu.solutions.a2.cdc.oracle",
      "fields" : [ {
        "type" : "int64",
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
        "type" : "int16",
        "optional" : false,
        "field" : "instance_number"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "instance_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "host_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "version"
      }, {
        "type" : "int64",
        "optional" : false,
        "field" : "ts_ms"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "owner"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "table"
      }, {
        "type" : "int64",
        "optional" : false,
        "field" : "scn"
      } ]
    }, {
      "type" : "string",
      "optional" : false,
      "field" : "op"
    }, {
      "type" : "int64",
      "optional" : false,
      "field" : "ts_ms"
    } ]
  },
  "payload" : {
    "before" : null,
    "after" : {
      "DEPTNO" : 20,
      "DNAME" : "RESEARCH",
      "LOC" : "DALLAS"
    },
    "source" : {
      "ts_ms" : 1569178592721,
      "owner" : "SCOTT",
      "table" : "DEPT",
      "scn" : 2288632,
      "dbid" : 3346093197,
      "database_name" : "JDK8",
      "platform_name" : "Linux x86 64-bit",
      "instance_number" : 1,
      "instance_name" : "JDK8",
      "host_name" : "kafka.wine-gu.ru",
      "version" : "12.2.0.1.0"
    },
    "op" : "u",
    "ts_ms" : 1569178592722
  }
}
```
### 'd' - DELETE message example

```
{
  "schema" : {
    "type" : "struct",
    "optional" : false,
    "name" : "SCOTT.DEPT.Envelope",
    "fields" : [ {
      "type" : "struct",
      "optional" : true,
      "field" : "before",
      "name" : "SCOTT.DEPT.PK",
      "fields" : [ {
        "type" : "int8",
        "optional" : false,
        "field" : "DEPTNO"
      } ]
    }, {
      "type" : "struct",
      "optional" : true,
      "field" : "after",
      "name" : "SCOTT.DEPT.Data",
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
      } ]
    }, {
      "type" : "struct",
      "optional" : false,
      "field" : "source",
      "name" : "eu.solutions.a2.cdc.oracle",
      "fields" : [ {
        "type" : "int64",
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
        "type" : "int16",
        "optional" : false,
        "field" : "instance_number"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "instance_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "host_name"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "version"
      }, {
        "type" : "int64",
        "optional" : false,
        "field" : "ts_ms"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "owner"
      }, {
        "type" : "string",
        "optional" : false,
        "field" : "table"
      }, {
        "type" : "int64",
        "optional" : false,
        "field" : "scn"
      } ]
    }, {
      "type" : "string",
      "optional" : false,
      "field" : "op"
    }, {
      "type" : "int64",
      "optional" : false,
      "field" : "ts_ms"
    } ]
  },
  "payload" : {
    "before" : {
      "DEPTNO" : 30
    },
    "after" : null,
    "source" : {
      "ts_ms" : 1569178592500,
      "owner" : "SCOTT",
      "table" : "DEPT",
      "scn" : 2991403,
      "dbid" : 3346093197,
      "database_name" : "JDK8",
      "platform_name" : "Linux x86 64-bit",
      "instance_number" : 1,
      "instance_name" : "JDK8",
      "host_name" : "kafka.wine-gu.ru",
      "version" : "12.2.0.1.0"
    },
    "op" : "d",
    "ts_ms" : 1569178592730
  }
}
```
