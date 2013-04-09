package shark.execution.serialization

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.io.Text
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import org.apache.hadoop.conf.Configuration
import com.ning.compress.lzf.LZFEncoder
import com.ning.compress.lzf.LZFDecoder

object HiveConfSerializer {

  def serialize(hConf: HiveConf): Array[Byte] = {
    val os = new ByteArrayOutputStream
    val dos = new DataOutputStream(os)
    val auxJars = hConf.getAuxJars()
    Text.writeString(dos, if(auxJars == null) "" else auxJars)
    hConf.write(dos)
    LZFEncoder.encode(os.toByteArray())
  }
  
  def deserialize(b: Array[Byte]): HiveConf = {
    val is = new ByteArrayInputStream(LZFDecoder.decode(b))
    val dis = new DataInputStream(is)
    val auxJars = Text.readString(dis)
    val conf = new HiveConf
    conf.readFields(dis)
    if(auxJars.equals("").unary_!)
      conf.setAuxJars(auxJars)
    conf
    
  }
}