package testArgs4j;

import org.kohsuke.args4j.Option;

public class ParseArgs implements java.io.Serializable {

    @Option(name="-outputPath",usage="the outpath we want to out")
    public String outputPath="";

    @Option(name="-outputType",usage="txt,csv,qually,or other")
    public String outputType="";

    @Option(name="-outputSplit",usage="the outputsplit, " +
            "if assign,move the success")
    public String outputSplit="";

    @Override
    public String toString() {
        return "ParseArgs{" +
                "outputPath='" + outputPath + '\'' +
                ", outputType='" + outputType + '\'' +
                ", outputSplit='" + outputSplit + '\'' +
                '}';
    }
}
