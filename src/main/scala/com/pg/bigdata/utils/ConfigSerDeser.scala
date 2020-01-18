package com.pg.bigdata.utils
import org.apache.hadoop.conf.Configuration

/**
  * Class to make Hadoop configurations serializable; uses the
  * `Writeable` operations to do this.
  * Note: this only serializes the explicitly set values, not any set
  * in site/default or other XML resources.
  * @param conf - hadoop configuration
  */
class ConfigSerDeser(var conf: Configuration) extends Serializable {

  def this() {
    this(new Configuration())
  }

  def get(): Configuration = conf

  private def writeObject (out: java.io.ObjectOutputStream): Unit = {
    conf.write(out)
  }

  private def readObject (in: java.io.ObjectInputStream): Unit = {
    conf = new Configuration()
    conf.readFields(in)
  }

  private def readObjectNoData(): Unit = {
    conf = new Configuration()
  }
}