package pers.prover07.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 计算一个要给定基本数据类型的长度
 *
 * @author Prover07
 * @date 2023/4/17 15:11
 */
public class LengthCountUDF extends GenericUDF {

    /**
     * 判断传进来的参数的类型和长度
     * 约定返回的数据类型
     *
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("please give me  only one arg");
        }
        // 判断是否是基础数据类型
        if (!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw  new UDFArgumentTypeException(1, "i need primitive type arg");
        }
        // 确定返回的数据类型(这里是 int)
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 主要执行逻辑
     * @param arguments
     *          The arguments as DeferedObject, use DeferedObject.get() to get the
     *          actual argument Object. The Objects can be inspected by the
     *          ObjectInspectors passed in the initialize call.
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object o = arguments[0].get();
        if(o==null){
            return 0;
        }

        return o.toString().length();
    }

    /**
     * 获取用于解释的字符串
     * @param strings
     * @return
     */
    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}
