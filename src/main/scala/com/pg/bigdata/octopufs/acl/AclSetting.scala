package com.pg.bigdata.octopufs.acl

import org.apache.hadoop.fs.permission.AclStatus

case class AclSetting(path: String, aclStatus: AclStatus)
