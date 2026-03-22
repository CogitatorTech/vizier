const std = @import("std");

// ============================================================================
// Types
// ============================================================================

pub const TokenKind = enum {
    keyword,
    identifier,
    number,
    string_lit,
    operator,
    comma,
    dot,
    lparen,
    rparen,
    star,
    semicolon,
    eof,
};

pub const Token = struct {
    kind: TokenKind,
    text: []const u8,
};

pub const PredicateKind = enum {
    equality,
    range,
    in_list,
    like,
    group_by,
    order_by,

    pub fn toStr(self: PredicateKind) []const u8 {
        return switch (self) {
            .equality => "equality",
            .range => "range",
            .in_list => "in_list",
            .like => "like",
            .group_by => "group_by",
            .order_by => "order_by",
        };
    }
};

pub const Predicate = struct {
    table_name: []const u8,
    column_name: []const u8,
    kind: PredicateKind,
};

pub const TableRef = struct {
    name: []const u8,
    alias: ?[]const u8,
};

const MAX_TOKENS = 1024;
const MAX_TABLES = 32;
const MAX_PREDICATES = 64;

pub const ExtractionResult = struct {
    tables: [MAX_TABLES]TableRef = undefined,
    table_count: usize = 0,
    predicates: [MAX_PREDICATES]Predicate = undefined,
    predicate_count: usize = 0,

    pub fn addTable(self: *ExtractionResult, name: []const u8, alias: ?[]const u8) void {
        if (self.table_count < MAX_TABLES) {
            self.tables[self.table_count] = .{ .name = name, .alias = alias };
            self.table_count += 1;
        }
    }

    pub fn addPredicate(self: *ExtractionResult, table: []const u8, column: []const u8, kind: PredicateKind) void {
        if (self.predicate_count < MAX_PREDICATES) {
            self.predicates[self.predicate_count] = .{ .table_name = table, .column_name = column, .kind = kind };
            self.predicate_count += 1;
        }
    }

    pub fn tableSlice(self: *const ExtractionResult) []const TableRef {
        return self.tables[0..self.table_count];
    }

    pub fn predicateSlice(self: *const ExtractionResult) []const Predicate {
        return self.predicates[0..self.predicate_count];
    }

    /// Resolve an alias to its table name, or return the alias itself if not found.
    pub fn resolveAlias(self: *const ExtractionResult, alias: []const u8) []const u8 {
        for (self.tableSlice()) |t| {
            if (t.alias) |a| {
                if (eqlIgnoreCase(a, alias)) return t.name;
            }
        }
        return alias;
    }

    /// Get the default table name (first table) for unqualified columns.
    pub fn defaultTable(self: *const ExtractionResult) []const u8 {
        if (self.table_count > 0) return self.tables[0].name;
        return "";
    }
};

// ============================================================================
// Tokenizer
// ============================================================================

const keywords = [_][]const u8{
    "SELECT", "FROM",   "JOIN",   "LEFT",    "RIGHT",  "INNER",
    "OUTER",  "CROSS",  "WHERE",  "AND",     "OR",     "ON",
    "IN",     "LIKE",   "BETWEEN","GROUP",   "ORDER",  "BY",
    "NOT",    "AS",     "HAVING", "LIMIT",   "INSERT", "UPDATE",
    "DELETE", "SET",    "INTO",   "VALUES",  "IS",     "NULL",
    "EXISTS", "NATURAL","FULL",   "DISTINCT","UNION",  "EXCEPT",
    "INTERSECT","CASE", "WHEN",   "THEN",    "ELSE",   "END",
};

pub fn isKeyword(word: []const u8) bool {
    for (keywords) |kw| {
        if (eqlLower(word, kw)) return true;
    }
    return false;
}

fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ac, bc| {
        if (std.ascii.toUpper(ac) != std.ascii.toUpper(bc)) return false;
    }
    return true;
}

fn eqlLower(a: []const u8, upper_b: []const u8) bool {
    if (a.len != upper_b.len) return false;
    for (a, upper_b) |ac, bc| {
        if (std.ascii.toUpper(ac) != bc) return false;
    }
    return true;
}

fn isIdentStart(c: u8) bool {
    return std.ascii.isAlphabetic(c) or c == '_';
}

fn isIdentChar(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
}

pub fn tokenize(sql: []const u8) struct { tokens: [MAX_TOKENS]Token, count: usize } {
    var tokens: [MAX_TOKENS]Token = undefined;
    var count: usize = 0;
    var i: usize = 0;

    while (i < sql.len and count < MAX_TOKENS - 1) {
        // Skip whitespace
        while (i < sql.len and std.ascii.isWhitespace(sql[i])) : (i += 1) {}
        if (i >= sql.len) break;

        const c = sql[i];

        // Line comment
        if (c == '-' and i + 1 < sql.len and sql[i + 1] == '-') {
            while (i < sql.len and sql[i] != '\n') : (i += 1) {}
            continue;
        }

        // Block comment
        if (c == '/' and i + 1 < sql.len and sql[i + 1] == '*') {
            i += 2;
            while (i + 1 < sql.len) {
                if (sql[i] == '*' and sql[i + 1] == '/') {
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }

        // String literal
        if (c == '\'') {
            const start = i;
            i += 1;
            while (i < sql.len) {
                if (sql[i] == '\'' and i + 1 < sql.len and sql[i + 1] == '\'') {
                    i += 2; // escaped quote
                } else if (sql[i] == '\'') {
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            }
            tokens[count] = .{ .kind = .string_lit, .text = sql[start..i] };
            count += 1;
            continue;
        }

        // Quoted identifier
        if (c == '"') {
            const start = i + 1;
            i += 1;
            while (i < sql.len and sql[i] != '"') : (i += 1) {}
            const end = i;
            if (i < sql.len) i += 1; // skip closing quote
            tokens[count] = .{ .kind = .identifier, .text = sql[start..end] };
            count += 1;
            continue;
        }

        // Identifier or keyword
        if (isIdentStart(c)) {
            const start = i;
            while (i < sql.len and isIdentChar(sql[i])) : (i += 1) {}
            const word = sql[start..i];
            const kind: TokenKind = if (isKeyword(word)) .keyword else .identifier;
            tokens[count] = .{ .kind = kind, .text = word };
            count += 1;
            continue;
        }

        // Number
        if (std.ascii.isDigit(c)) {
            const start = i;
            while (i < sql.len and (std.ascii.isDigit(sql[i]) or sql[i] == '.')) : (i += 1) {}
            tokens[count] = .{ .kind = .number, .text = sql[start..i] };
            count += 1;
            continue;
        }

        // Two-char operators
        if (i + 1 < sql.len) {
            const two = sql[i .. i + 2];
            if (std.mem.eql(u8, two, "<=") or std.mem.eql(u8, two, ">=") or
                std.mem.eql(u8, two, "<>") or std.mem.eql(u8, two, "!="))
            {
                tokens[count] = .{ .kind = .operator, .text = two };
                count += 1;
                i += 2;
                continue;
            }
        }

        // Single-char tokens
        switch (c) {
            '=' => {
                tokens[count] = .{ .kind = .operator, .text = sql[i .. i + 1] };
                count += 1;
                i += 1;
            },
            '<', '>' => {
                tokens[count] = .{ .kind = .operator, .text = sql[i .. i + 1] };
                count += 1;
                i += 1;
            },
            '.' => {
                tokens[count] = .{ .kind = .dot, .text = "." };
                count += 1;
                i += 1;
            },
            ',' => {
                tokens[count] = .{ .kind = .comma, .text = "," };
                count += 1;
                i += 1;
            },
            '(' => {
                tokens[count] = .{ .kind = .lparen, .text = "(" };
                count += 1;
                i += 1;
            },
            ')' => {
                tokens[count] = .{ .kind = .rparen, .text = ")" };
                count += 1;
                i += 1;
            },
            '*' => {
                tokens[count] = .{ .kind = .star, .text = "*" };
                count += 1;
                i += 1;
            },
            ';' => {
                tokens[count] = .{ .kind = .semicolon, .text = ";" };
                count += 1;
                i += 1;
            },
            else => {
                i += 1; // skip unknown
            },
        }
    }

    tokens[count] = .{ .kind = .eof, .text = "" };
    count += 1;

    return .{ .tokens = tokens, .count = count };
}

// ============================================================================
// Extraction
// ============================================================================

fn isKw(tok: Token, upper: []const u8) bool {
    return tok.kind == .keyword and eqlLower(tok.text, upper);
}

fn isJoinKw(tok: Token) bool {
    return isKw(tok, "JOIN") or isKw(tok, "LEFT") or isKw(tok, "RIGHT") or
        isKw(tok, "INNER") or isKw(tok, "OUTER") or isKw(tok, "CROSS") or
        isKw(tok, "NATURAL") or isKw(tok, "FULL");
}

/// Main extraction entry point — pure function, no DB access.
pub fn extractFromSql(sql: []const u8) ExtractionResult {
    const tok_result = tokenize(sql);
    const tokens = tok_result.tokens[0..tok_result.count];
    var result = ExtractionResult{};

    // Pass 1: extract tables
    extractTables(tokens, &result);

    // Pass 2: extract predicates
    extractPredicates(tokens, &result);

    return result;
}

fn extractTables(tokens: []const Token, result: *ExtractionResult) void {
    var i: usize = 0;
    while (i < tokens.len) {
        // Look for FROM or JOIN keywords
        if (isKw(tokens[i], "FROM") or isKw(tokens[i], "JOIN")) {
            i += 1;
            // Skip extra JOIN modifiers (e.g., LEFT OUTER JOIN — we already consumed JOIN,
            // but if we matched LEFT, skip until JOIN)
            if (isKw(tokens[i -| 1], "FROM")) {
                // nothing extra to skip
            } else {
                // We matched JOIN, proceed
            }
            // After FROM/JOIN, expect table name (skip lparen for subqueries)
            if (i < tokens.len and tokens[i].kind == .lparen) {
                // Subquery — skip to matching rparen
                var depth: usize = 1;
                i += 1;
                while (i < tokens.len and depth > 0) {
                    if (tokens[i].kind == .lparen) depth += 1;
                    if (tokens[i].kind == .rparen) depth -= 1;
                    i += 1;
                }
                // After closing paren, there may be an alias
                if (i < tokens.len and tokens[i].kind == .identifier) {
                    i += 1; // skip subquery alias
                } else if (i < tokens.len and isKw(tokens[i], "AS") and i + 1 < tokens.len and tokens[i + 1].kind == .identifier) {
                    i += 2;
                }
                continue;
            }
            if (i >= tokens.len or tokens[i].kind != .identifier) continue;

            var table_name = tokens[i].text;
            i += 1;

            // Check for schema.table — use just the table part for simplicity
            if (i + 1 < tokens.len and tokens[i].kind == .dot and tokens[i + 1].kind == .identifier) {
                table_name = tokens[i + 1].text;
                i += 2;
            }

            // Check for alias: next identifier that's not a keyword
            var alias: ?[]const u8 = null;
            if (i < tokens.len and isKw(tokens[i], "AS") and i + 1 < tokens.len and tokens[i + 1].kind == .identifier) {
                alias = tokens[i + 1].text;
                i += 2;
            } else if (i < tokens.len and tokens[i].kind == .identifier and !isJoinKw(tokens[i]) and !isKw(tokens[i], "ON") and !isKw(tokens[i], "WHERE") and !isKw(tokens[i], "GROUP") and !isKw(tokens[i], "ORDER") and !isKw(tokens[i], "HAVING") and !isKw(tokens[i], "LIMIT") and !isKw(tokens[i], "UNION") and !isKw(tokens[i], "EXCEPT") and !isKw(tokens[i], "INTERSECT")) {
                alias = tokens[i].text;
                i += 1;
            }

            result.addTable(table_name, alias);
            continue;
        }

        // Skip JOIN modifiers (LEFT, RIGHT, etc.) — they precede JOIN
        if (isJoinKw(tokens[i]) and !isKw(tokens[i], "JOIN")) {
            i += 1;
            // Skip until we hit JOIN
            while (i < tokens.len and !isKw(tokens[i], "JOIN") and isJoinKw(tokens[i])) : (i += 1) {}
            continue; // the JOIN keyword will be caught next iteration
        }

        i += 1;
    }
}

fn extractPredicates(tokens: []const Token, result: *ExtractionResult) void {
    var i: usize = 0;

    while (i < tokens.len) {
        // WHERE / AND / OR — expect predicate LHS
        if (isKw(tokens[i], "WHERE") or isKw(tokens[i], "AND") or isKw(tokens[i], "OR") or isKw(tokens[i], "ON")) {
            i += 1;
            // Skip NOT
            if (i < tokens.len and isKw(tokens[i], "NOT")) i += 1;
            // Skip EXISTS
            if (i < tokens.len and isKw(tokens[i], "EXISTS")) {
                i += 1;
                continue;
            }

            if (i >= tokens.len or tokens[i].kind != .identifier) continue;

            // Parse column reference: possibly prefix.column
            var table_name: []const u8 = result.defaultTable();
            var column_name = tokens[i].text;
            i += 1;

            if (i + 1 < tokens.len and tokens[i].kind == .dot and tokens[i + 1].kind == .identifier) {
                // prefix.column — resolve prefix
                table_name = result.resolveAlias(column_name);
                column_name = tokens[i + 1].text;
                i += 2;
            }

            // Determine predicate kind from the operator/keyword that follows
            if (i >= tokens.len) continue;

            if (tokens[i].kind == .operator) {
                const op = tokens[i].text;
                if (std.mem.eql(u8, op, "=") or std.mem.eql(u8, op, "!=") or std.mem.eql(u8, op, "<>")) {
                    result.addPredicate(table_name, column_name, .equality);
                } else {
                    result.addPredicate(table_name, column_name, .range);
                }
                i += 1;
            } else if (isKw(tokens[i], "IN")) {
                result.addPredicate(table_name, column_name, .in_list);
                i += 1;
            } else if (isKw(tokens[i], "LIKE")) {
                result.addPredicate(table_name, column_name, .like);
                i += 1;
            } else if (isKw(tokens[i], "BETWEEN")) {
                result.addPredicate(table_name, column_name, .range);
                i += 1;
            } else if (isKw(tokens[i], "IS")) {
                // IS NULL / IS NOT NULL — treat as equality
                result.addPredicate(table_name, column_name, .equality);
                i += 1;
            }
            continue;
        }

        // GROUP BY
        if (isKw(tokens[i], "GROUP") and i + 1 < tokens.len and isKw(tokens[i + 1], "BY")) {
            i += 2;
            while (i < tokens.len) {
                if (tokens[i].kind != .identifier) break;
                var tbl = result.defaultTable();
                var col = tokens[i].text;
                i += 1;
                if (i + 1 < tokens.len and tokens[i].kind == .dot and tokens[i + 1].kind == .identifier) {
                    tbl = result.resolveAlias(col);
                    col = tokens[i + 1].text;
                    i += 2;
                }
                result.addPredicate(tbl, col, .group_by);
                // Skip comma
                if (i < tokens.len and tokens[i].kind == .comma) {
                    i += 1;
                } else {
                    break;
                }
            }
            continue;
        }

        // ORDER BY
        if (isKw(tokens[i], "ORDER") and i + 1 < tokens.len and isKw(tokens[i + 1], "BY")) {
            i += 2;
            while (i < tokens.len) {
                if (tokens[i].kind != .identifier) break;
                var tbl = result.defaultTable();
                var col = tokens[i].text;
                i += 1;
                if (i + 1 < tokens.len and tokens[i].kind == .dot and tokens[i + 1].kind == .identifier) {
                    tbl = result.resolveAlias(col);
                    col = tokens[i + 1].text;
                    i += 2;
                }
                result.addPredicate(tbl, col, .order_by);
                // Skip ASC/DESC
                if (i < tokens.len and (isKw(tokens[i], "ASC") or isKw(tokens[i], "DESC"))) i += 1;
                // Skip NULLS FIRST/LAST
                if (i < tokens.len and isKw(tokens[i], "NULL")) i += 1; // NULLS
                if (i < tokens.len and tokens[i].kind == .identifier) i += 1; // FIRST/LAST
                // Skip comma
                if (i < tokens.len and tokens[i].kind == .comma) {
                    i += 1;
                } else {
                    break;
                }
            }
            continue;
        }

        i += 1;
    }
}

// ============================================================================
// JSON builders
// ============================================================================

pub fn buildTablesJson(result: *const ExtractionResult, buf: []u8) []const u8 {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();
    w.writeByte('[') catch return "[]";
    for (result.tableSlice(), 0..) |t, idx| {
        if (idx > 0) w.writeByte(',') catch return "[]";
        w.print("\"{s}\"", .{t.name}) catch return "[]";
    }
    w.writeByte(']') catch return "[]";
    return fbs.getWritten();
}

pub fn buildColumnsJson(result: *const ExtractionResult, buf: []u8) []const u8 {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();
    w.writeByte('[') catch return "[]";
    var written: usize = 0;
    for (result.predicateSlice()) |p| {
        // Skip group_by and order_by for the columns JSON — just filter/join columns
        if (p.kind == .group_by or p.kind == .order_by) continue;
        if (written > 0) w.writeByte(',') catch return "[]";
        w.print("\"{s}.{s}\"", .{ p.table_name, p.column_name }) catch return "[]";
        written += 1;
    }
    w.writeByte(']') catch return "[]";
    return fbs.getWritten();
}

// ============================================================================
// Tests
// ============================================================================

fn findPredicate(result: *const ExtractionResult, table: []const u8, column: []const u8, kind: PredicateKind) bool {
    for (result.predicateSlice()) |p| {
        if (std.mem.eql(u8, p.table_name, table) and
            std.mem.eql(u8, p.column_name, column) and
            p.kind == kind) return true;
    }
    return false;
}

fn findTable(result: *const ExtractionResult, name: []const u8) bool {
    for (result.tableSlice()) |t| {
        if (std.mem.eql(u8, t.name, name)) return true;
    }
    return false;
}

test "simple equality" {
    const r = extractFromSql("SELECT * FROM orders WHERE customer_id = 42");
    try std.testing.expect(findTable(&r, "orders"));
    try std.testing.expectEqual(@as(usize, 1), r.table_count);
    try std.testing.expect(findPredicate(&r, "orders", "customer_id", .equality));
}

test "join with aliases" {
    const r = extractFromSql("SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.total > 100 AND c.country = 'US'");
    try std.testing.expect(findTable(&r, "orders"));
    try std.testing.expect(findTable(&r, "customers"));
    try std.testing.expectEqual(@as(usize, 2), r.table_count);
    try std.testing.expect(findPredicate(&r, "orders", "total", .range));
    try std.testing.expect(findPredicate(&r, "customers", "country", .equality));
}

test "IN list" {
    const r = extractFromSql("SELECT * FROM products WHERE category IN ('electronics', 'books')");
    try std.testing.expect(findPredicate(&r, "products", "category", .in_list));
}

test "BETWEEN" {
    const r = extractFromSql("SELECT * FROM events WHERE ts BETWEEN '2024-01-01' AND '2024-12-31'");
    try std.testing.expect(findPredicate(&r, "events", "ts", .range));
}

test "LIKE" {
    const r = extractFromSql("SELECT * FROM users WHERE email LIKE '%@gmail.com'");
    try std.testing.expect(findPredicate(&r, "users", "email", .like));
}

test "GROUP BY and ORDER BY" {
    const r = extractFromSql("SELECT department, COUNT(*) FROM employees WHERE active = true GROUP BY department ORDER BY department");
    try std.testing.expect(findPredicate(&r, "employees", "active", .equality));
    try std.testing.expect(findPredicate(&r, "employees", "department", .group_by));
    try std.testing.expect(findPredicate(&r, "employees", "department", .order_by));
}

test "string containing keywords not parsed" {
    const r = extractFromSql("SELECT * FROM logs WHERE message = 'SELECT FROM WHERE'");
    try std.testing.expect(findTable(&r, "logs"));
    try std.testing.expectEqual(@as(usize, 1), r.table_count);
    try std.testing.expect(findPredicate(&r, "logs", "message", .equality));
}

test "multiple conditions" {
    const r = extractFromSql("SELECT * FROM t WHERE a = 1 AND b > 2 AND c IN (3, 4)");
    try std.testing.expect(findPredicate(&r, "t", "a", .equality));
    try std.testing.expect(findPredicate(&r, "t", "b", .range));
    try std.testing.expect(findPredicate(&r, "t", "c", .in_list));
    try std.testing.expectEqual(@as(usize, 3), r.predicate_count);
}

test "tables json builder" {
    const r = extractFromSql("SELECT * FROM orders o JOIN customers c ON o.cid = c.id");
    var buf: [256]u8 = undefined;
    const json = buildTablesJson(&r, &buf);
    try std.testing.expectEqualStrings("[\"orders\",\"customers\"]", json);
}

test "columns json builder" {
    const r = extractFromSql("SELECT * FROM orders WHERE customer_id = 42 AND amount > 100");
    var buf: [256]u8 = undefined;
    const json = buildColumnsJson(&r, &buf);
    try std.testing.expectEqualStrings("[\"orders.customer_id\",\"orders.amount\"]", json);
}

// --- Regression tests for bugs found during development ---

test "regression: alias resolution is case-insensitive" {
    // Bug: eqlLower expected uppercase second arg, but aliases are lowercase.
    // resolveAlias("o") failed to match alias "o" stored from "FROM orders o".
    const r = extractFromSql("SELECT o.id FROM orders o WHERE o.total > 100");
    try std.testing.expect(findPredicate(&r, "orders", "total", .range));
}

test "regression: tokenizer handles >= operator correctly" {
    // Ensure two-char operators like >= don't get split into > and =
    const r = extractFromSql("SELECT * FROM events WHERE ts >= '2024-01-01'");
    try std.testing.expect(findPredicate(&r, "events", "ts", .range));
    try std.testing.expectEqual(@as(usize, 1), r.predicate_count);
}

test "regression: empty SQL produces no results" {
    const r = extractFromSql("");
    try std.testing.expectEqual(@as(usize, 0), r.table_count);
    try std.testing.expectEqual(@as(usize, 0), r.predicate_count);
}

test "regression: SQL with only keywords" {
    const r = extractFromSql("SELECT FROM WHERE");
    try std.testing.expectEqual(@as(usize, 0), r.table_count);
}

test "LEFT JOIN extracts table" {
    const r = extractFromSql("SELECT * FROM a LEFT JOIN b ON a.id = b.aid WHERE b.x = 1");
    try std.testing.expect(findTable(&r, "a"));
    try std.testing.expect(findTable(&r, "b"));
    try std.testing.expectEqual(@as(usize, 2), r.table_count);
}

test "IS NULL treated as equality predicate" {
    const r = extractFromSql("SELECT * FROM t WHERE col IS NULL");
    try std.testing.expect(findPredicate(&r, "t", "col", .equality));
}

test "NOT does not break predicate extraction" {
    const r = extractFromSql("SELECT * FROM t WHERE NOT active = false");
    try std.testing.expect(findPredicate(&r, "t", "active", .equality));
}

test "quoted identifier" {
    const r = extractFromSql("SELECT * FROM \"my table\" WHERE \"my col\" = 1");
    try std.testing.expect(findTable(&r, "my table"));
    try std.testing.expect(findPredicate(&r, "my table", "my col", .equality));
}

test "line comment is ignored" {
    const r = extractFromSql("SELECT * FROM t -- this is a comment\nWHERE x = 1");
    try std.testing.expect(findTable(&r, "t"));
    try std.testing.expect(findPredicate(&r, "t", "x", .equality));
}

test "block comment is ignored" {
    const r = extractFromSql("SELECT * FROM t /* comment */ WHERE x = 1");
    try std.testing.expect(findTable(&r, "t"));
    try std.testing.expect(findPredicate(&r, "t", "x", .equality));
}
