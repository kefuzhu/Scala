package HadoopRun

/**
  * Created by Kefu on 10/12/2017.
  */
object HDFS{

  def deleteFile(path: String) : Unit = {
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}
    val hdfs: FileSystem = FileSystem.get(new Configuration)
    val isExist = hdfs.exists(new Path(path))
    if (isExist) {
      hdfs.delete(new Path(path), true) //true: delete files recursively
    }
  }

  def main(args: Array[String]): Unit = {
    println("File deleted successfully!")
  }
}
