using System;
using System.Collections.Generic;
using System.Linq;

// ==========================================
// 1. CORE TYPE SYSTEM (Polymorphism)
// ==========================================
public abstract class DataType
{
    public abstract bool Validate(object value);
}

public class IntegerType : DataType
{
    public override bool Validate(object value) => value is int;
}

public class StringType : DataType
{
    public override bool Validate(object value) => value is string;
}

public class BoolType : DataType
{
    public override bool Validate(object value) => value is bool;
}

// ==========================================
// 2. DATA STRUCTURES
// ==========================================
public class Column
{
    public string Name { get; set; }
    public DataType Type { get; set; }
    public bool IsNullable { get; set; } = true;
    public Tuple<string, string> ForeignKey { get; set; } // (TableName, ColumnName)
}

public class Row(Dictionary<string, object> data, int id = 0)
{
    public int Id { get; set; } = id;
    public Dictionary<string, object> Data { get; set; } = data;
}

// ==========================================
// 3. TABLE LOGIC (The Heavy Lifter)
// ==========================================
public class Table
{
    public string Name { get; set; }
    public Dictionary<string, Column> Columns { get; set; }
    public List<Row> Rows { get; set; }
    private int nextId = 1;
    private List<Row> _backupRows; // For Transactions

    public Table(string name, List<Column> columns)
    {
        Name = name;
        Columns = [];
        foreach (var column in columns)
            Columns[column.Name] = column;
        Rows = [];
    }

    // Overload for Anonymous Types (Syntactic Sugar)
    public Row Insert(object data)
    {
        var dict = new Dictionary<string, object>();
        foreach (var prop in data.GetType().GetProperties())
            dict[prop.Name] = prop.GetValue(data);
        return Insert(dict);
    }

    // Main Insert Logic
    public Row Insert(Dictionary<string, object> rowData)
    {
        foreach (var columnName in Columns.Keys)
        {
            var column = Columns[columnName];
            rowData.TryGetValue(columnName, out var value);

            // A. Validate Type
            if (!column.Type.Validate(value) && (!column.IsNullable || value != null))
                throw new ArgumentException(
                    $"[Validation] Column '{columnName}' expected {column.Type.GetType().Name}, got {value}"
                );

            // B. Validate Foreign Key
            if (column.ForeignKey != null && value != null)
            {
                var (refTable, refCol) = column.ForeignKey;
                var db = Database.Instance;

                if (!db.Tables.TryGetValue(refTable, out var tableRef))
                    throw new Exception($"[FK Error] Table '{refTable}' not found.");

                // Check existence in target table
                bool exists = tableRef.Rows.Any(r =>
                    r.Data.TryGetValue(refCol, out var val) && val.Equals(value)
                );

                if (!exists)
                    throw new Exception(
                        $"[FK Violation] Key '{value}' not found in {refTable}.{refCol}"
                    );
            }
        }

        var row = new Row(rowData, nextId++);
        Rows.Add(row);
        return row;
    }

    public SimpleQuery Query() => new SimpleQuery(this);

    // --- Transactions ---
    public void BeginTransaction() => _backupRows = [.. Rows];

    public void Commit() => _backupRows = null;

    public void Rollback()
    {
        if (_backupRows != null)
        {
            Rows = _backupRows;
            _backupRows = null;
        }
    }
}

// ==========================================
// 4. JOIN LOGIC
// ==========================================
public class JoinedTable(Table left, Table right)
{
    public List<Row> Join(string leftCol, string rightCol)
    {
        var result = new List<Row>();

        foreach (var lRow in left.Rows)
        {
            if (!lRow.Data.TryGetValue(leftCol, out var lVal))
                continue;

            foreach (var rRow in right.Rows)
            {
                if (!rRow.Data.TryGetValue(rightCol, out var rVal))
                    continue;

                if (lVal.Equals(rVal))
                {
                    // Merge Data
                    var merged = new Dictionary<string, object>(lRow.Data);
                    foreach (var kvp in rRow.Data)
                    {
                        // Handle name collisions by prefixing
                        string key = merged.ContainsKey(kvp.Key)
                            ? $"{right.Name}_{kvp.Key}"
                            : kvp.Key;
                        merged[key] = kvp.Value;
                    }
                    result.Add(new Row(merged));
                }
            }
        }
        return result;
    }
}

// ==========================================
// 5. QUERY ENGINE
// ==========================================
public class SimpleQuery(Table table)
{
    private readonly Table _table = table;
    private readonly List<(string Col, string Op, object Val)> _conditions = [];
    private readonly List<string> _selection = [];

    public SimpleQuery Select(params string[] cols)
    {
        _selection.AddRange(cols);
        return this;
    }

    public SimpleQuery Where(string col, string op, object val)
    {
        _conditions.Add((col, op, val));
        return this;
    }

    public List<Row> Execute()
    {
        var results = new List<Row>();
        foreach (var row in _table.Rows)
        {
            bool match = true;
            foreach (var (col, op, target) in _conditions)
            {
                if (!row.Data.TryGetValue(col, out var actual))
                {
                    match = false;
                    break;
                }

                bool pass = op switch
                {
                    "==" => actual.Equals(target),
                    ">" => (dynamic)actual > (dynamic)target,
                    "<" => (dynamic)actual < (dynamic)target,
                    _ => false,
                };
                if (!pass)
                {
                    match = false;
                    break;
                }
            }

            if (match)
            {
                var data =
                    _selection.Count == 0
                        ? new Dictionary<string, object>(row.Data)
                        : _selection
                            .Where(k => row.Data.ContainsKey(k))
                            .ToDictionary(k => k, k => row.Data[k]);
                results.Add(new Row(data, row.Id));
            }
        }
        return results;
    }
}

// ==========================================
// 6. CREATIONAL PATTERNS (Builder & Factory)
// ==========================================
public class TableBuilder(string tableName)
{
    public string TableName { get; set; } = tableName;
    private readonly List<Column> _columns = [];

    public TableBuilder AddColumn(string name, DataType type, bool isNullable = true)
    {
        _columns.Add(
            new Column
            {
                Name = name,
                Type = type,
                IsNullable = isNullable,
            }
        );
        return this;
    }

    public TableBuilder AddForeignKey(string name, DataType type, string refTable, string refCol)
    {
        _columns.Add(
            new Column
            {
                Name = name,
                Type = type,
                IsNullable = false,
                ForeignKey = new Tuple<string, string>(refTable, refCol),
            }
        );
        return this;
    }

    public Table Build() => new(TableName, _columns);
}

public static class TableFactory
{
    public static Table CreateTable(string name, Dictionary<string, string> schema)
    {
        var builder = new TableBuilder(name);
        foreach (var kvp in schema)
        {
            DataType type = kvp.Value.ToLower() switch
            {
                "int" => new IntegerType(),
                "string" => new StringType(),
                "bool" => new BoolType(),
                _ => throw new ArgumentException($"Unknown type: {kvp.Value}"),
            };
            builder.AddColumn(kvp.Key, type);
        }
        return builder.Build();
    }
}

// ==========================================
// 7. DATABASE SINGLETON
// ==========================================
public class Database
{
    private static readonly Lazy<Database> instance = new(() => new Database());
    public static Database Instance => instance.Value;

    private Database() { }

    public Dictionary<string, Table> Tables { get; set; } = [];

    public void CreateTable(Table table)
    {
        // Prevent creation if FK table doesn't exist
        foreach (var col in table.Columns.Values)
            if (col.ForeignKey != null && !Tables.ContainsKey(col.ForeignKey.Item1))
                throw new Exception(
                    $"Cannot create '{table.Name}': Referenced table '{col.ForeignKey.Item1}' missing."
                );

        Tables[table.Name] = table;
    }
}

// ==========================================
// 8. FINAL DEMONSTRATION
// ==========================================
class Program
{
    static void Main()
    {
        Console.WriteLine("=== LAB 4: DATABASE ENGINE DEMO ===\n");
        var db = Database.Instance;

        // --- STEP 1: Factory Pattern ---
        Console.WriteLine("1. Creating 'Users' table using Factory...");
        var userSchema = new Dictionary<string, string> { { "Id", "int" }, { "Name", "string" } };
        var users = TableFactory.CreateTable("Users", userSchema);
        db.CreateTable(users);

        users.Insert(new { Id = 1, Name = "Alice" });
        users.Insert(new { Id = 2, Name = "Bob" });
        Console.WriteLine($"   -> Success. Users count: {users.Rows.Count}");

        // --- STEP 2: Builder Pattern & Foreign Keys ---
        Console.WriteLine("\n2. Creating 'Orders' table using Builder (with Foreign Key)...");
        var orders = new TableBuilder("Orders")
            .AddColumn("OrderId", new IntegerType())
            .AddForeignKey("UserId", new IntegerType(), "Users", "Id") // FK -> Users.Id
            .AddColumn("Amount", new IntegerType())
            .Build();
        db.CreateTable(orders);
        Console.WriteLine("   -> Success. Table 'Orders' linked to 'Users'.");

        // --- STEP 3: Referential Integrity (FK Check) ---
        Console.WriteLine("\n3. Testing Foreign Key Validation...");
        try
        {
            orders.Insert(
                new
                {
                    OrderId = 100,
                    UserId = 99,
                    Amount = 500,
                }
            ); // User 99 does not exist
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   -> Blocked Invalid Insert: {ex.Message}");
        }

        orders.Insert(
            new
            {
                OrderId = 101,
                UserId = 1,
                Amount = 150,
            }
        ); // Valid (User 1 exists)
        orders.Insert(
            new
            {
                OrderId = 102,
                UserId = 2,
                Amount = 300,
            }
        ); // Valid (User 2 exists)
        Console.WriteLine($"   -> Valid rows inserted. Orders count: {orders.Rows.Count}");

        // --- STEP 4: Validation System ---
        Console.WriteLine("\n4. Testing Data Type Validation...");
        try
        {
            users.Insert(new { Id = 3, Name = 12345 }); // Name should be String, passed Int
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   -> Blocked Invalid Type: {ex.Message}");
        }

        // --- STEP 5: Complex Query ---
        Console.WriteLine("\n5. Executing Query: Find Orders with Amount > 200...");
        var bigOrders = orders
            .Query()
            .Where("Amount", ">", 200)
            .Select("OrderId", "UserId") // Select specific columns
            .Execute();

        foreach (var row in bigOrders)
            Console.WriteLine(
                $"   -> Found Order #{row.Data["OrderId"]} (User {row.Data["UserId"]})"
            );

        // --- STEP 6: Transactions ---
        Console.WriteLine("\n6. Testing Transactions (Commit/Rollback)...");
        users.BeginTransaction();
        users.Insert(new { Id = 99, Name = "Ghost User" });
        Console.WriteLine(
            $"   -> Inside Transaction. Users count: {users.Rows.Count} (Ghost present)"
        );

        Console.WriteLine("   -> Rolling Back...");
        users.Rollback();
        Console.WriteLine($"   -> Rollback Complete. Users count: {users.Rows.Count} (Ghost gone)");

        // --- STEP 7: Inner Join ---
        Console.WriteLine("\n7. Testing JoinedTable (Users + Orders)...");
        var joiner = new JoinedTable(orders, users);
        // Join Orders.UserId on Users.Id
        var report = joiner.Join("UserId", "Id");

        foreach (var row in report)
        {
            // Note: Users.Name is merged into the result
            Console.WriteLine($"   -> JOIN RESULT: {row.Data["Name"]} spent ${row.Data["Amount"]}");
        }

        Console.WriteLine("\n=== DEMO COMPLETE ===");
    }
}
