package shark

import com.esotericsoftware.kryo.Kryo
import de.javakaffee.kryoserializers.ArraysAsListSerializer
import java.util.Arrays

class KryoRegistrator extends spark.KryoRegistrator {
  def registerClasses(kryo: Kryo) {

    kryo.register(classOf[exec.ReduceKey])

    // Java Arrays.asList returns an internal class that cannot be serialized
    // by default Kryo. This provides a workaround.
    kryo.register(Arrays.asList().getClass, new ArraysAsListSerializer(kryo))
  }
}
