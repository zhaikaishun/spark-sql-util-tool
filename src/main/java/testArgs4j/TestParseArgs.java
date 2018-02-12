package testArgs4j;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public class TestParseArgs {
    public static void main(String[] args) {
        ParseArgs parseArgs = new ParseArgs();
        CmdLineParser pars = new CmdLineParser(parseArgs);
        if(args.length==0){
            System.out.println("args is 0");
            return;
        }
        try {
            pars.parseArgument(args);
            System.out.println(parseArgs.toString());


        } catch (CmdLineException e) {
            e.printStackTrace();
        }

    }
}
