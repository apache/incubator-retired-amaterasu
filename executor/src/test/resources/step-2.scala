import org.apache.amaterasu.executor.runtime.AmaContext


val highNoDf = AmaContext.getDataFrame("start", "x").where("age > 20")
highNoDf.show
