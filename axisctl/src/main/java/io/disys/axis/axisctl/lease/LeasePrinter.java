package io.disys.axis.axisctl.lease;

import io.disys.axis.api.proto.LeaseDetail;
import io.disys.axis.api.proto.LeaseInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class LeasePrinter {

    static void printLeaseTable(List<LeaseInfo> leases) {
        if (leases.isEmpty()) {
            System.out.println("(no active leases)");
            return;
        }
        System.out.printf("%d lease%s%n", leases.size(), leases.size() == 1 ? "" : "s");

        var headers = List.of("LEASE-ID", "TTL(s)", "REMAINING(s)");
        var rows = new ArrayList<List<String>>();
        for (var lease : leases) {
            rows.add(List.of(
                    String.valueOf(lease.getLeaseId()),
                    String.valueOf(lease.getTtl()),
                    String.valueOf(lease.getRemainingTtl())));
        }

        int n = headers.size();
        int[] w = new int[n];
        for (int i = 0; i < n; i++) w[i] = headers.get(i).length();
        for (var row : rows)
            for (int i = 0; i < n; i++) w[i] = Math.max(w[i], row.get(i).length());

        var sep = separator(w);
        System.out.println();
        System.out.println(sep);
        System.out.println(row(headers, w));
        System.out.println(sep);
        for (var row : rows) System.out.println(row(row, w));
        System.out.println(sep);
    }

    static void printLeaseInfo(LeaseDetail d) {
        System.out.printf("lease-id:  %d%n", d.getLeaseId());
        System.out.printf("ttl:       %ds%n", d.getTtl());
        System.out.printf("remaining: %ds%n", d.getRemainingTtl());
        if (d.getKeysCount() == 0) {
            System.out.println("keys:      (none)");
        } else {
            var keys = d.getKeysList().stream()
                    .map(k -> k.toStringUtf8())
                    .collect(Collectors.joining(", "));
            System.out.printf("keys:      %s%n", keys);
        }
    }

    private static String separator(int[] widths) {
        var sb = new StringBuilder("+");
        for (int w : widths) sb.append("-".repeat(w + 2)).append("+");
        return sb.toString();
    }

    private static String row(List<String> cells, int[] widths) {
        var sb = new StringBuilder("|");
        for (int i = 0; i < cells.size(); i++)
            sb.append(" ").append(String.format("%-" + widths[i] + "s", cells.get(i))).append(" |");
        return sb.toString();
    }
}
