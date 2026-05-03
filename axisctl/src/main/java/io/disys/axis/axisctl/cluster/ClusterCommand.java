package io.disys.axis.axisctl.cluster;

import picocli.CommandLine.Command;

@Command(
        name = "cluster",
        mixinStandardHelpOptions = true,
        description = "Cluster lifecycle management.",
        subcommands = {
                StartCommand.class,
                StopCommand.class,
                StatusCommand.class,
                LogsCommand.class
        }
)
public class ClusterCommand implements Runnable {

    @Override
    public void run() {
        picocli.CommandLine.usage(this, System.out);
    }
}
