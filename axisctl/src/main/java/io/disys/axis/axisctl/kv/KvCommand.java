package io.disys.axis.axisctl.kv;

import picocli.CommandLine.Command;

@Command(
        name = "kv",
        mixinStandardHelpOptions = true,
        description = "Key-value operations.",
        subcommands = {
                PutCommand.class,
                GetCommand.class,
                DelCommand.class,
                RangeCommand.class,
                UpdateValueCommand.class,
                UpdateLeaseCommand.class,
                RemoveLeaseCommand.class
        }
)
public class KvCommand implements Runnable {

    @Override
    public void run() {
        picocli.CommandLine.usage(this, System.out);
    }
}
