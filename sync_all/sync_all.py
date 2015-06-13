#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb
import re
import time
import os
from subprocess import Popen,PIPE
import logging
from optparse import OptionParser
import Queue
import threading
import sys
reload(sys)
sys.setdefaultencoding( "gbk" )

logger = logging.getLogger('sync_all')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('sync_all.log')
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
#ch.setLevel(logging.INFO)

batch_size=1000
formatter = logging.Formatter('%(asctime)s - %(levelname)s -[%(filename)s: %(lineno)s ]- %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)

class TargetDB:
    def __init__(self, targetHost,targetPort,targetSchema,targetCharset,targetUser,targetPasswd):
        self.targetHost=targetHost
        self.targetPort=targetPort
        self.targetSchema=targetSchema
        self.targetCharset=targetCharset
        self.targetUser=targetUser
        self.targetPasswd=targetPasswd
        self.conn = None

    def connect(self):
        if self.conn is not None:
            self.conn.close()
            self.conn=None
        self.conn=MySQLdb.connect(host=self.targetHost,port=int(self.targetPort),
                                db=self.targetSchema,user=self.targetUser,passwd=self.targetPasswd,connect_timeout=5,charset=self.targetCharset)
        self.conn.autocommit(False)

    def execute(self, sql):
        try:
            cursor = self.conn.cursor()
            affected_rows=cursor.execute(sql)
        except (AttributeError, MySQLdb.OperationalError):
            self.connect()
            cursor = self.conn.cursor()
            affected_rows=cursor.execute(sql)
        return (cursor,affected_rows)
       
    def execute(self, sql, para_list):
        try:
            cursor = self.conn.cursor()
            affected_rows=cursor.execute(sql,para_list)
        except (AttributeError, MySQLdb.OperationalError):
            self.connect()
            cursor = self.conn.cursor()
            affected_rows=cursor.execute(sql,para_list)
        return (cursor,affected_rows)
        
    def commit(self):
        self.conn.commit()
        
    def rollback(self):
        self.conn.rollback()

class MigThread(threading.Thread):
    def __init__(self, name, source_db, source_sql,source_col, source_key, source_opt, target_db):
        threading.Thread.__init__(self)
        self.name=name
        
        self.sourceHost=source_db["sourceHost"]
        self.sourcePort=source_db["sourcePort"]
        self.sourceSchema=source_db["sourceSchema"]
        self.sourceUser=source_db["sourceUser"]
        self.sourcePasswd=source_db["sourcePasswd"]
        self.sourceCharset=source_db["sourceCharset"]
        
        self.sourceTable=source_opt["source_tab"]
        
        self.sourceTablePK=source_key
        self.sourceTableAllColumnList=None
        self.sourceTrigTableName=None
        self.sourceConn=None
        
        self.sourceSql=source_sql
        self.sourceColumn=source_col 
        
        self.targetDBPart=source_opt["target_db_part"]
        self.targetTablePart=source_opt["target_tab_part"]
        self.targetTablePrefix=source_opt["target_tab_prefix"]
        
        self.targetDBDic=target_db   #dic
        self.targetConnDic={}
        
        self.setInit()
        
        self.skipCheck=False
        self.type=None
        self.remove=False
        
    def buildDBConn(self):
        try:
            self.sourceConn=MySQLdb.connect(host=self.sourceHost,port=int(self.sourcePort),
                                db=self.sourceSchema,user=self.sourceUser,passwd=self.sourcePasswd,connect_timeout=5,charset=self.sourceCharset)
            self.sourceConn.autocommit(True)
            for key in self.targetDBDic.keys():
                targetHost=self.targetDBDic[key]["targetHost"]
                targetPort=self.targetDBDic[key]["targetPort"]
                targetSchema=self.targetDBDic[key]["targetSchema"]
                targetCharset=self.targetDBDic[key]["targetCharset"]
                targetUser=self.targetDBDic[key]["targetUser"]
                targetPasswd=self.targetDBDic[key]["targetPasswd"]
                self.targetConnDic[key]=TargetDB(targetHost,targetPort,targetSchema,targetCharset,targetUser,targetPasswd)
        except MySQLdb.MySQLError, e:
            logger.error("MySQL-Error[%d] %s"%(e.args[0],e.args[1]))
            exit(1)
            
    def skipPreCheck(self):
        self.skipCheck=True
        
    def setRunType(self,type):
        self.type=type
        
    def setRemove(self):
        self.remove=True
            
    def setInit(self):
        self.sourceTrigTableName="__trig_%s"%(self.sourceTable)
        
        if self.sourceConn is None:
            self.buildDBConn()
        
        try:
            self.sourceTableAllColumnList=self.getColumnList()
        except MySQLdb.MySQLError, e:
            logger.error("MySQL-Error[%d] %s"%(e.args[0],e.args[1]))
            exit(1)
        
        if self.sourceTablePK is None or self.sourceTableAllColumnList is None or self.sourceTrigTableName is None or self.sourceConn is None:
            logger.error("setInit error, some variables None")
            exit(1)

    def run(self):
        if self.remove :
            self.removeTrigger()
            return True
        if not self.skipCheck and not self.preCheck():
            logger.error(self.sourceTable+"  preCheck Fail")
            return False
        if self.buildTrigger():
            if self.type in ("both","full"):
                if self.syncFullTable():
                    logger.info(self.sourceTable+"  syncFullTable Success: %s"%(self.name))
                else:
                    logger.error(self.sourceTable+"  syncFullTable Fail: %s"%(self.name))
                    return False
            if self.type in ( "both", "incre"):
                if self.syncIncreTable():
                    logger.info(self.sourceTable+"  syncIncreTable Success: %s"%(self.name))
                else:
                    logger.error(self.sourceTable+"  syncIncreTable Fail: %s"%(self.name))
                    return False
            if self.type not in ( "both", "incre" , "full" ):
                logger.error(self.sourceTable+"  unsupport type: %s"%(str(self.type)))
                return False
        else:
            logger.error(self.sourceTable+"  buildTrigger Fail: %s"%(self.name))
            return False
        
    def preCheck(self):
        """return True if preCheck condition OK, False otherwise"""
        sql="""select count(*) from information_schema.triggers where EVENT_OBJECT_SCHEMA='%(db)s' and EVENT_OBJECT_TABLE='%(table)s' """%{
              "db":self.sourceSchema,
              "table":self.sourceTable
             }
        try:
            cur=self.sourceConn.cursor()
            rows=cur.execute(sql)
            rs=cur.fetchall()
            if rs is not None and len(rs)==1:
                cnt=int(rs[0][0])
                if cnt !=0:
                    logger.warn("trigger on %s.%s already exists, check first"%(self.sourceSchema,self.sourceTable))
                    return False
                else:
                    return True
            else:
                logger.error("error preCheck")
                return False
        except MySQLdb.MySQLError, e:
            logger.error("MySQL-Error[%d] %s"%(e.args[0],e.args[1]))
            exit(1)
        return False
        
    def buildTrigger(self):
        """return True if success, False otherwise"""
        trig_table_ddl=self.getTrigTableDDL() 
        logger.debug(trig_table_ddl)

        columnList=self.sourceTableAllColumnList
        trig_insert="""
        create trigger trig_insert_%(table)s after insert on %(table)s for each row 
                begin 
                        insert into %(trig_table)s (%(all_columns)s)
                        values(null,'I',%(new)s); 
                end
                """%{"table":self.sourceTable,
                     "trig_table":self.sourceTrigTableName,
                     "all_columns":"trig_sequence,dml_type,"+",".join(columnList),
                     "new":",".join(["new."+c for c in columnList])
                }
        trig_update="""
        create trigger trig_update_%(table)s after update on %(table)s for each row 
        begin 
            insert into %(trig_table)s (%(all_columns)s)
            values(null,'U',%(new)s); 
        end
        """%{"table":self.sourceTable,
             "trig_table":self.sourceTrigTableName,
             "all_columns":"trig_sequence,dml_type,"+",".join(columnList),
             "new":",".join(["new."+c for c in columnList])
        }
        trig_delete="""
        create trigger trig_delete_%(table)s after delete on %(table)s for each row 
        begin 
            insert into %(trig_table)s (%(all_columns)s)
            values(null,'D',%(old)s); 
        end
        """%{"table":self.sourceTable,
             "trig_table":self.sourceTrigTableName,
             "all_columns":"trig_sequence,dml_type,"+",".join(columnList),
             "old":",".join(["old."+c for c in columnList])
        }
        logger.debug(trig_insert)
        logger.debug(trig_update)
        logger.debug(trig_delete)

        try:
            cur=self.sourceConn.cursor()
            cur.execute(trig_table_ddl)
            cur.execute(trig_insert)
            cur.execute(trig_update)
            cur.execute(trig_delete)
            cur.close() 
            logger.info("trig DDL DONE")
        except MySQLdb.MySQLError, e:
            logger.error("MySQL-Error[%d] %s"%(e.args[0],e.args[1]))
        return True 
        
    def getTrigTableDDL(self):
        sqlDDL="""
create table %(trigTableName)s (
    trig_sequence bigint unsigned auto_increment primary key
   , dml_type varchar(1) not null 
%(column_defination)s
 )
            """
        try:
            sql="""desc %(db)s.%(table)s"""%{"db":self.sourceSchema,"table":self.sourceTable}
            cur=self.sourceConn.cursor()
            affect_rows=cur.execute(sql)
            rs=cur.fetchall()
            column_defination=""
            for r in rs:
                field=r[0]
                type =r[1]
                Null=r[2]
                column="   , %s %s %s \n"%(field,type,"not null" if Null=="NO" else "")
                column_defination=column_defination+column
            cur.close()
            sqlDDL=sqlDDL%{"trigTableName":self.sourceTrigTableName,"column_defination":column_defination}
        except MySQLdb.MySQLError, e:
            logger.error("MySQL-Error[%d] %s"%(e.args[0],e.args[1]))
            exit(1)
        return sqlDDL

    def getColumnList(self):
        sql="""select group_concat(COLUMN_NAME) from information_schema.columns where TABLE_SCHEMA='%(db)s' and TABLE_NAME='%(table)s'  order by ORDINAL_POSITION;    """%{"db":self.sourceSchema,"table":self.sourceTable}
        logger.debug(sql)
        cur=self.sourceConn.cursor()
        affect_rows=cur.execute(sql)
        rs=cur.fetchall()
        print(rs)
        if rs is not None and len(rs)==1:
            return rs[0][0].split(",")
        else:
            logger.error("can't get column list")
            exit(1)

    def getColumnsHolder(self, columns):
        return ",".join(['%s' for i in range(columns.count(",")+1)]) 

    def insertTarget(self,db_part,table_part,data):
        """for syncFullTable"""
        try:
            sql="insert into %(table_prefix)s%(table_part)s (%(columns)s) values( %(columns_holder)s)"%{
                "table_prefix":self.targetTablePrefix,
                "table_part":table_part,
                "columns":self.sourceColumn,
                "columns_holder":self.getColumnsHolder(self.sourceColumn)
                }
            logger.debug(sql)
            (cur,affected_rows)=self.targetConnDic[db_part].execute(sql,data)
            self.targetConnDic[db_part].commit()
            cur.close()
            if affected_rows !=1:
                logger.warn("insert error:%s"%(",".join(map(str,data))))
                return False
            else:
                return True
        except MySQLdb.MySQLError, e:
            logger.error("MySQL-Error[%d] %s"%(e.args[0],e.args[1]))
            self.targetConnDic[db_part].rollback()
            return False
    
    def syncFullTable(self):
        """return True if success, False otherwise"""
        logger.info(self.sourceTable+"  syncFullTable begin")
        try:
            sql="""
        select %(db_part)s, %(table_part)s, %(columns)s
        %(source_sql)s
        """%{"db_part":self.targetDBPart,
             "table_part":self.targetTablePart,
             "columns":self.sourceColumn,
             "source_sql":self.sourceSql}
            logger.debug(self.sourceTable+"  syncFullTable SQL:%s"%(sql))
            cur=self.sourceConn.cursor(MySQLdb.cursors.SSCursor)
            cur.arraysize=batch_size
            cur.execute(sql)
            lines=0
            try:
                rs=cur.fetchmany()
            except UnicodeDecodeError, e:
                logger.error("lines:%d"%(lines))
                logger.error("UnicodeDecodeError")
                rs=cur.fetchmany()
            while len(rs)!=0:
                for r in rs:
                    lines=lines+1
                    if lines%500==0:
                        logger.info(self.sourceTable+"  processing %d"%(lines))
                    db_part=r[0]
                    table_part=r[1]
                    data=r[2:]
                    if not self.insertTarget(db_part,table_part,data):
                        logger.error(self.sourceTable+"  FAIL TRANS:%s"%(",".join(map(str,r))))    
                try:
                    rs=cur.fetchmany()
                except UnicodeDecodeError, e:
                    logger.error("lines:%d"%(lines))
                    logger.error("UnicodeDecodeError")
                    rs=cur.fetchmany()
            logger.info(self.sourceTable+"  %d lines"%(lines))
        except MySQLdb.MySQLError, e:
            logger.error(self.sourceTable+"  MySQL-Error[%d] %s"%(e.args[0],e.args[1]))
            return False

        logger.info("syncFullTable end")
        return True
 
    def formWhere(self, col,col_holder):
        col_list=col.split(",")
        col_holder_list=col_holder.split(",")
        if len(col_list) > 0 and len(col_list) == len(col_holder_list):
            rt=[]
            for i in range(len(col_list)):
                rt.append(col_list[i]+"="+col_holder_list[i])
            return " and ".join(rt)
        else:
            logger.error("formWhere error %s, %s"%(col, col_holder))
            return "1=0"
     
    def syncTarget(self, dml_type, db_part,table_part, key_list, data_list):
        """for syncIncreTable, return True if sync OK"""
        try:
            logger.debug(self.sourceTable+"  syncTarget  %s %s"%(db_part,table_part))
            #cur=self.targetConnDic[db_part].cursor()
            if dml_type in ( "I","U" ):
                if dml_type == "U":
                    delete_sql="delete from %(table)s%(table_part)s where %(where)s "%{
                    "table":self.targetTablePrefix,
                    "table_part":table_part,
                    "where":self.formWhere(self.sourceTablePK,self.getColumnsHolder(self.sourceTablePK))
                    }
                    logger.debug(delete_sql)
                    (cur,rows)=self.targetConnDic[db_part].execute(delete_sql, key_list)
                    logger.debug("delete rows:%d"%(rows))
                    if rows == 0:
                        logger.warn(self.sourceTable+"  no rows deleted for %s, pk: %s"%(self.targetTablePrefix+table_part, ",".join(map(str,key_list))))
                logger.debug(self.targetTablePrefix)
                logger.debug(table_part)
                logger.debug(self.sourceColumn)
                logger.debug(self.getColumnsHolder(self.sourceColumn))
                insert_sql="insert into %(table)s%(table_part)s (%(column)s) values( %(column_holder)s )"%{
                    "table":self.targetTablePrefix,
                    "table_part":table_part,
                    "column":self.sourceColumn,
                    "column_holder":self.getColumnsHolder(self.sourceColumn)
                }
                logger.debug(insert_sql)
                #rows=cur.execute(insert_sql, data_list)
                (cur,rows)=self.targetConnDic[db_part].execute(insert_sql, data_list)
                if rows == 0:
                    logger.warn(self.sourceTable+"  no rows inserted for %s, pk: %s"%(self.targetTablePrefix+table_part, ",".join(map(str,key_list))))
                    self.targetConnDic[db_part].rollback()
                    cur.close()
                    return False
                else:
                    self.targetConnDic[db_part].commit()
                    cur.close()
                    return True
            elif dml_type == 'D':
                delete_sql="delete from %(table)s%(table_part)s where %(where)s "%{
                    "table":self.targetTablePrefix,
                    "table_part":table_part,
                    "where":self.formWhere(self.sourceTablePK,self.getColumnsHolder(self.sourceTablePK))
                    }
                logger.debug(delete_sql)
                #rows=cur.execute(delete_sql, key_list)
                (cur,rows)=self.targetConnDic[db_part].execute(delete_sql, key_list)
                if rows == 0:
                    logger.warn(self.sourceTable+"  no rows deleted for %s, pk: %s"%(self.targetTablePrefix+table_part, ",".join(map(str,key_list))))
                    self.targetConnDic[db_part].rollback()
                    cur.close()
                    return False
                else:
                    self.targetConnDic[db_part].commit()
                    cur.close()
                    return True
            else:
                logger.error(self.sourceTable+"  DML type error:%s"%(dml_type))
                return False
        except MySQLdb.MySQLError, e:
            logger.error(self.sourceTable+"  MySQL-Error[%d] %s"%(e.args[0],e.args[1]))
            if e.args[0] == 1062:
                self.targetConnDic[db_part].commit();
                cur.close()
                return True
            else:
                self.targetConnDic[db_part].rollback()
                cur.close()
                return False
        
    def syncIncreTable(self):
        """return True if success, False otherwise"""
        logger.info(self.sourceTrigTableName+"  syncIncreTable Begin")
        columns=self.sourceColumn
        select_trig="""select %(columns)s from %(trig_tab_name)s order by trig_sequence limit %(limit)d"""%{
            "columns":"trig_sequence,dml_type,"+self.targetDBPart+","+self.targetTablePart+","+self.sourceTablePK+","+columns,
            "trig_tab_name":self.sourceTrigTableName,
            "limit":50
        }
        logger.debug(select_trig)
        try:
            while True:
                time.sleep(1)
                cur=self.sourceConn.cursor()
                rows=cur.execute(select_trig)
                if rows==0:
                    logger.warn(self.sourceTrigTableName+"  No rows selected")
                    cur.close()
                    continue
                rs=cur.fetchall()
                seq_list=[]
                for r in rs:
                    trig_seq=r[0]
                    dml_type=r[1]
                    db_part=r[2]
                    tab_part=r[3]
                    pk=r[4:(4+self.sourceTablePK.count(",")+1)]
                    data=r[(4+self.sourceTablePK.count(",")+1):]
                    logger.debug(self.sourceTrigTableName+"  data:%s"%("\n".join(map(str,r))))
                    if self.syncTarget(dml_type,db_part,tab_part, pk, data):
                        seq_list.append(trig_seq)
                    else:
                        logger.warn(self.sourceTrigTableName+"  Error syncIncreTable %s"%(",".join(map(str,r))))
                cur.close()
                if len(seq_list)==0:
                    logger.debug(self.sourceTrigTableName+"  seq_list empty!")
                    continue 
                delete_trig="""delete from %(trig_tab_name)s where trig_sequence in (%(seq_list)s) """%{
                    "trig_tab_name":self.sourceTrigTableName,
                    "seq_list":",".join(map(str,seq_list))
                }
                logger.debug(self.sourceTrigTableName+"  delete_trig:%s"%(delete_trig))
                cur=self.sourceConn.cursor()
                rows=cur.execute(delete_trig)
                if rows==0:
                    logger.warn(self.sourceTrigTableName+"  No rows delete affected, Check the query first")
                else:
                    logger.info(self.sourceTrigTableName+"  syncIncreTable processing %d rows"%(rows))
                cur.close()
        except MySQLdb.MySQLError, e:
            logger.error(self.sourceTrigTableName+"  MySQL-Error[%d] %s"%(e.args[0],e.args[1]))
            return False
        logger.info(self.sourceTrigTableName+"  syncIncreTable End")
        return True
        
    def removeTrigger(self):
        """return True if success, False otherwise"""
        logger.info(self.sourceTrigTableName+"  will not drop!")
        logger.info(self.sourceTrigTableName+"  removeTrigger Begin")

        trig_insert="""drop trigger trig_insert_%(table)s ;"""%{"table":self.sourceTable}
        trig_update="""drop trigger trig_update_%(table)s ;"""%{"table":self.sourceTable}
        trig_delete="""drop trigger trig_delete_%(table)s ;"""%{"table":self.sourceTable}

        try:
            cur=self.sourceConn.cursor()
            cur.execute(trig_insert)
            cur.execute(trig_update)
            cur.execute(trig_delete)
            cur.close() 
            logger.info(self.sourceTrigTableName+"  removeTrigger Done")
        except MySQLdb.MySQLError, e:
            logger.error(self.sourceTrigTableName+"  MySQL-Error[%d] %s"%(e.args[0],e.args[1]))

    def stopRun(self):
        """return True if success, False otherwise"""
        logger.info("stopRun")
        #TODO
        return True

def getConf(file):
    conf=open(file,'r')
    lines=conf.readlines()
    source_db=None
    source_sql={}
    source_opt={}
    source_col={}
    source_key={}
    target_db={}
    for l in lines:
        line=l.strip()
        if line == "" or line.startswith("#"):
            continue
        if line.count("=")<=0:
            logger.error("error config file format, key=value for each line")
            exit(1)
        kv=line.split("=",1)
        key=kv[0].strip()
        if key == "source_db":
            if source_db is not None:
                logger.error("duplicate source_db defination, err")
                exit(1)
            val=kv[1].strip()
            (sourceType, sourceHost, sourcePort, sourceSchema, sourceCharset, sourceUser, sourcePasswd)=val.split("#")
            if sourceType !="mysql":
                logger.error("unsupport sourceType:%s, only mysql now"%(sourceType))
                exit(1)
            source_db={"sourceHost":sourceHost,
                      "sourcePort":sourcePort,
                      "sourceSchema":sourceSchema,
                      "sourceUser":sourceUser,
                      "sourceCharset":sourceCharset,
                      "sourcePasswd":sourcePasswd
                      }
        elif key.startswith("source_sql_"):
            m=re.search("(?<=source_sql_)\d+",key)
            if m is None:
                logger.error("error config file format, source_sql_(\d+)=values")
                exit(1)
            idx=m.group(0)
            if source_sql.has_key(idx):
                logger.error("duplicate source_sql defination [%s], err"%(idx))
                exit(1)
            source_sql[idx]=kv[1].strip()
        elif key.startswith("source_opt_"):
            m=re.search("(?<=source_opt_)\d+",key)
            if m is None:
                logger.error("error config file format, source_opt_(\d+)=values")
                exit(1)
            idx=m.group(0)
            if source_opt.has_key(idx):
                logger.error("duplicate source_opt defination [%s], err"%(idx))
                exit(1)
            
            val=kv[1].strip()
            (source_tab, target_db_part, target_tab_part, target_tab_prefix)=val.split("#")
            source_opt[idx]={"source_tab":source_tab,
                             "target_db_part":target_db_part,
                             "target_tab_part":target_tab_part,
                             "target_tab_prefix":target_tab_prefix                
                    }
        elif key.startswith("source_col_"):
            m=re.search("(?<=source_col_)\d+",key)
            if m is None:
                logger.error("error config file format, source_col_(\d+)=values")
                exit(1)
            idx=m.group(0)
            if source_col.has_key(idx):
                logger.error("duplicate source_col defination [%s], err"%(idx))
                exit(1)
            source_col[idx]=kv[1].strip()
        elif key.startswith("source_key_"):
            m=re.search("(?<=source_key_)\d+",key)
            if m is None:
                logger.error("error config file format, source_key_(\d+)=values")
                exit(1)
            idx=m.group(0)
            if source_key.has_key(idx):
                logger.error("duplicate source_key defination [%s], err"%(idx))
                exit(1)
            source_key[idx]=kv[1].strip() 
        elif key.startswith("target_db_"):
            m=re.search("(?<=target_db_)\d+",key)
            if m is None:
                logger.error("error config file format, target_db_(\d+)=values")
                exit(1)
            idx=m.group(0)
            if target_db.has_key(idx):
                logger.error("duplicate target_db defination [%s], err"%(idx))
                exit(1)
            val=kv[1].strip()
            (targetType, targetHost, targetPort, targetSchema, targetCharset, targetUser, targetPasswd)=val.split("#")
            if targetType !="mysql":
                logger.error("unsupport targetType:%s, only mysql now"%(targetType))
                exit(1)
            target_db[idx]={"targetHost":targetHost,
                      "targetPort":targetPort,
                      "targetSchema":targetSchema,
                      "targetCharset":targetCharset,
                      "targetUser":targetUser,
                      "targetPasswd":targetPasswd
                      }
        else:
            logger.error("unsupport file format:%s"%(line))
            exit(1)
    if source_db is None or len(source_sql)==0 or len(source_opt)==0 or len(source_col)==0 or len(target_db)==0:
        logger.error("no enough info in config file, check source_db/source_sql/mig_option/target_db")
        exit(1)

    for k in source_sql.keys():
        if not source_opt.has_key(k) or not source_col.has_key(k):
            logger.error("source_opt or source_col has no such key(%s), which source_sql holds"%(k))
            exit(1)
            
    return {"source_db":source_db,
            "source_sql":source_sql,
            "source_col":source_col,
            "source_key":source_key,
            "source_opt":source_opt,
            "target_db":target_db
           }

def parseCli():
    parser = OptionParser()
    parser.add_option("-f", "--configfile", dest="file",
                    help="config file", metavar="FILE")
    parser.add_option("--skip-precheck", dest="precheck",
                    help="skip precheck", action="store_true", metavar="PRECHECK") 
    parser.add_option("-t", "--type", dest="type",choices=["both","full","incre"],
                    help="do full sync or incremental sync or both. choice: both,full,incre", metavar="TYPE")
    parser.add_option("-r", "--remove", dest="remove",
                    help="remove all triggers and tmp tables(should be empty)", action="store_true",
                    metavar="REMOVE")
    return parser.parse_args()
    
def main():
    import fcntl, sys
    pid_file = 'sync_all.pid'
    fp = open(pid_file, 'w')
    try:
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        logger.error("one instance of sync_all is running")
        sys.exit(0)
        
    (options, args) = parseCli()
    if options.file is None:
        logger.error("sync_all.py -f <file> -t type [option]")
        exit(1)
    if not os.path.exists(options.file):
        logger.error("file %s doesn't exist!"%(options.file))
        exit(1)
    if  options.remove is None and  options.type is None:
        logger.error("sync_all.py -f <file> -t type [option]")
        exit(1)
    
    configDic=getConf(options.file)
    source_db=configDic["source_db"]
    source_sql=configDic["source_sql"]
    source_col=configDic["source_col"]
    source_key=configDic["source_key"]
    source_opt=configDic["source_opt"]
    target_db=configDic["target_db"]
    
    threadPool=[]
    for idx in source_sql.keys():
        name="sync_all%s"%(idx)
        handle_source_db=source_db
        handle_source_sql=source_sql[idx]
        handle_source_col=source_col[idx]
        handle_source_key=source_key[idx]
        handle_source_opt=source_opt[idx]
        handle_target_db=target_db
        thread=MigThread(name,handle_source_db,handle_source_sql,handle_source_col,handle_source_key, handle_source_opt ,handle_target_db)
        thread.setRunType(options.type)
        if options.precheck is not None:
            thread.skipPreCheck()
        if options.remove is not None:
            thread.setRemove()
        threadPool.append(thread)
        
    for t in threadPool:
        t.start()

    for t in threadPool:
        t.join()
    
if __name__ == "__main__":
    main()