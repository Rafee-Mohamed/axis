package io.disys.axis.axisctl;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.Parser;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliJLineCompleter;

import java.io.IOException;

public class Shell {

    static void run(CommandLine cmd) throws IOException {
        var terminal = TerminalBuilder.builder().system(true).build();
        var parser = new DefaultParser();
        var reader = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(new PicocliJLineCompleter(cmd.getCommandSpec()))
                .parser(parser)
                .build();

        var writer = terminal.writer();
        writer.println("axisctl interactive shell");
        writer.println("  commands : kv, lease, cluster");
        writer.println("  help     : <command> --help");
        writer.println("  quit     : exit or Ctrl+D");
        writer.flush();

        cmd.setExecutionExceptionHandler((ex, cmdLine, parseResult) -> {
            writer.println("error: " + ex.getMessage());
            writer.flush();
            return 1;
        });

        while (true) {
            String line;
            try {
                line = reader.readLine("axis> ");
            } catch (UserInterruptException e) {
                continue;
            } catch (EndOfFileException e) {
                break;
            }

            if (line == null) break;
            line = line.trim();
            if (line.isEmpty()) continue;
            if (line.equals("exit") || line.equals("quit")) break;

            var tokens = parser.parse(line, line.length(), Parser.ParseContext.SPLIT_LINE);
            cmd.execute(tokens.words().toArray(String[]::new));
        }

        terminal.close();
    }
}
