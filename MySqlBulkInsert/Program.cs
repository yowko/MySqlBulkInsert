// See https://aka.ms/new-console-template for more information

using System.Globalization;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Bogus;
using CsvHelper;
using CsvHelper.Configuration.Attributes;
using MySqlConnector;

Console.WriteLine("Hello, World!");

var summary = BenchmarkRunner.Run<MySqlService>();

[MemoryDiagnoser(true)]
public class MySqlService
{
    [Params(1000, 10000, 100000)] public int BatchSize { get; set; }

    [Benchmark]
    public void MySqlBulkLoaderCsv()
    {
        var filename = "./orders.csv";
        using var writer = new StreamWriter(filename);
        {
            using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);
            csv.WriteRecords(GetOrders(BatchSize));
        }
        var connectionString =
            "Server=localhost;Port=3306;Database=test;Username=root;Password=pass.123;Allow User Variables=true;AllowLoadLocalInfile=true;"; //
        using var connection = new MySqlConnection(connectionString);
        {
            connection.Open();

            var bl = new MySqlBulkLoader(connection)
            {
                TableName = "orders",
                FileName = filename,
                FieldTerminator = ",",
                LineTerminator = "\n",
                NumberOfLinesToSkip = 1,
                FieldQuotationOptional = true,
                Columns = { "order_date", "product_id", "order_type", "amount" }
            };

            var inserted = bl.Load();
            //Console.WriteLine(inserted + " rows inserted.");
        }
    }

    [Benchmark]
    public void MySqlBulkLoaderStream()
    {
        using var memoryStream = new MemoryStream();
        using var streamWriter = new StreamWriter(memoryStream);
        using var csvWriter = new CsvWriter(streamWriter, CultureInfo.InvariantCulture);
        csvWriter.WriteRecords(GetOrders(BatchSize));
        streamWriter.Flush();
        memoryStream.Position = 0;
        var connectionString =
            "Server=localhost;Port=3306;Database=test;Username=root;Password=pass.123;Allow User Variables=true;AllowLoadLocalInfile=true;"; //
        using var connection = new MySqlConnection(connectionString);
        {
            connection.Open();

            var bl = new MySqlBulkLoader(connection)
            {
                TableName = "orders",
                SourceStream = memoryStream,
                FieldTerminator = ",",
                LineTerminator = "\n",
                NumberOfLinesToSkip = 1,
                FieldQuotationOptional = true,
                Columns = { "order_date", "product_id", "order_type", "amount" }
            };

            var inserted = bl.Load();
            //Console.WriteLine(inserted + " rows inserted.");
        }
    }

    [Benchmark]
    public void MySqlInsert()
    {
        // 建立資料庫連線
        var connectionString =
            "Server=localhost;Port=3306;Database=test;Username=root;Password=pass.123;Allow User Variables=true;";
        using var connection = new MySqlConnection(connectionString);
        connection.Open();

        try
        {
            // 將資料存入資料表
            var insertStatement =
                new MySqlCommand(
                    "INSERT INTO orders (id, order_date, product_id, order_type, amount) VALUES (@id, @order_date, @product_id, @order_type, @amount)",
                    connection);
            foreach (var order in GetOrders(BatchSize))
            {
                insertStatement.Parameters.Clear();
                insertStatement.Parameters.AddWithValue("@order_date", order.OrderDate);
                insertStatement.Parameters.AddWithValue("@product_id", order.ProductId);
                insertStatement.Parameters.AddWithValue("@order_type", order.OrderType);
                insertStatement.Parameters.AddWithValue("@amount", order.Amount);
                insertStatement.ExecuteNonQuery();
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        finally
        {
            // 關閉資料庫連線
            connection.Close();
        }
    }

    Order[] GetOrders(int BatchSize)
    {
        var startDate = new DateTime(2021, 01, 01, 00, 00, 00, DateTimeKind.Utc);
        var order = new Faker<Order>()
            .RuleFor(a => a.Id, f => f.Random.ULong())
            .RuleFor(a => a.OrderDate, f => startDate.AddDays(f.Random.Number(0, 365 * 3)))
            .RuleFor(a => a.ProductId, f => f.Random.Number(1, 10000))
            .RuleFor(a => a.OrderType, f => f.Random.SByte(1, 10))
            .RuleFor(a => a.Amount, f => f.Random.Decimal(0M, 100000M));
        return order.Generate(BatchSize).ToArray();
    }
}


public class Order
{
    [Ignore]
    public ulong Id { get; set; }
    public DateTime OrderDate { get; set; }
    public int ProductId { get; set; }
    public sbyte OrderType { get; set; }
    public decimal Amount { get; set; }
}