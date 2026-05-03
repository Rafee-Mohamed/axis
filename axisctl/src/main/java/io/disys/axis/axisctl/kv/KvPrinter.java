package io.disys.axis.axisctl.kv;

import io.disys.axis.api.proto.KeyVal;
import io.disys.axis.api.proto.ResponseHeader;

import java.util.ArrayList;
import java.util.List;

class KvPrinter {

    static void printKv(KeyVal kv, ResponseHeader header) {
        System.out.printf("rev:%d%n", header.getRevision());
        printTable(List.of(kv));
    }

    static void printKvAt(KeyVal kv, long atRevision) {
        System.out.printf("rev:%d%n", atRevision);
        printTable(List.of(kv));
    }

    static void printNotFoundAt(long revision) {
        System.out.printf("rev:%d%n(not found)%n", revision);
    }

    static void printKvTable(List<KeyVal> kvs, ResponseHeader header, boolean more) {
        if (kvs.isEmpty()) {
            System.out.printf("rev:%d%n(empty)%n", header.getRevision());
            return;
        }
        System.out.printf("rev:%d  %d result%s%s%n",
                header.getRevision(), kvs.size(), kvs.size() == 1 ? "" : "s",
                more ? "  (more available)" : "");
        printTable(kvs);
    }

    static void printKvTableAt(List<KeyVal> kvs, long atRevision, boolean more) {
        if (kvs.isEmpty()) {
            System.out.printf("rev:%d%n(empty)%n", atRevision);
            return;
        }
        System.out.printf("rev:%d  %d result%s%s%n",
                atRevision, kvs.size(), kvs.size() == 1 ? "" : "s",
                more ? "  (more available)" : "");
        printTable(kvs);
    }

    static void printStored(String key, String value, ResponseHeader header, long leaseId) {
        if (leaseId != 0) {
            System.out.printf("stored  [rev:%d, lease:%d]%n", header.getRevision(), leaseId);
        } else {
            System.out.printf("stored  [rev:%d]%n", header.getRevision());
        }
    }

    static void printUpdated(ResponseHeader header) {
        System.out.printf("updated  [rev:%d]%n", header.getRevision());
    }

    static void printDeleted(String key, ResponseHeader header) {
        System.out.printf("deleted  [rev:%d]%n", header.getRevision());
    }

    static void printNotFound(String key, ResponseHeader header) {
        System.out.printf("not-found  [rev:%d]%n", header.getRevision());
    }

    static void printPrev(KeyVal prev) {
        System.out.printf("%nprev:%n");
        printTableNoBefore(List.of(prev));
    }

    static void printPrevMany(List<KeyVal> prevKvs) {
        System.out.printf("%nprev:%n");
        printTableNoBefore(prevKvs);
    }

    static void printPrevAbsent() {
        System.out.println("prev: (none)");
    }

    private static void printTable(List<KeyVal> kvs) {
        System.out.println();
        printTableNoBefore(kvs);
    }

    private static void printTableNoBefore(List<KeyVal> kvs) {
        boolean hasLease = kvs.stream().anyMatch(kv -> kv.getLeaseId() != 0);

        var headers = new ArrayList<>(List.of("KEY", "VALUE", "MODIFIED", "CREATED", "VERSION"));
        if (hasLease) headers.add("LEASE");

        var rows = new ArrayList<List<String>>();
        for (var kv : kvs) {
            var row = new ArrayList<>(List.of(
                    kv.getKey().toStringUtf8(),
                    kv.getVal().toStringUtf8(),
                    String.valueOf(kv.getModifiedRevision()),
                    String.valueOf(kv.getCreatedRevision()),
                    String.valueOf(kv.getVersion())));
            if (hasLease) row.add(kv.getLeaseId() != 0 ? String.valueOf(kv.getLeaseId()) : "-");
            rows.add(row);
        }

        int n = headers.size();
        int[] w = new int[n];
        for (int i = 0; i < n; i++) w[i] = headers.get(i).length();
        for (var row : rows)
            for (int i = 0; i < n; i++) w[i] = Math.max(w[i], row.get(i).length());

        var sep = separator(w);
        System.out.println(sep);
        System.out.println(row(headers, w));
        System.out.println(sep);
        for (var row : rows) System.out.println(row(row, w));
        System.out.println(sep);
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
