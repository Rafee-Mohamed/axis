package io.disys.axis.axisctl;

import io.disys.axis.axisctl.cluster.ClusterCommand;
import io.disys.axis.axisctl.kv.KvCommand;
import io.disys.axis.axisctl.lease.LeaseCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
        name = "axisctl",
        mixinStandardHelpOptions = true,
        version = "axisctl 0.1.0",
        description = "CLI for interacting with an Axis KV cluster.",
        subcommands = {
                KvCommand.class,
                LeaseCommand.class,
                ClusterCommand.class
        }
)
public class AxisCtl implements Runnable {

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    public static void main(String[] args) {
        int exit = new CommandLine(new AxisCtl()).execute(args);
        System.exit(exit);
    }
}
