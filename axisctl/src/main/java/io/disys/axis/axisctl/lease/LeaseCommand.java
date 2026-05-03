package io.disys.axis.axisctl.lease;

import picocli.CommandLine.Command;

@Command(
        name = "lease",
        mixinStandardHelpOptions = true,
        description = "Lease management.",
        subcommands = {
                GrantCommand.class,
                RevokeCommand.class,
                InfoCommand.class,
                ListCommand.class
        }
)
public class LeaseCommand implements Runnable {

    @Override
    public void run() {
        picocli.CommandLine.usage(this, System.out);
    }
}
