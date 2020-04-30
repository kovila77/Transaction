using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;

namespace Transaction
{
    class Program
    {

        static void Main(string[] args)
        {
            var sConnStr = new NpgsqlConnectionStringBuilder
            {
                Host = "192.168.88.102",
                Port = 5432,
                Database = "outpost2",
                Username = "postgres",
                Password = "kovila77",
            }.ConnectionString;

            Console.WriteLine("Тест \"Неповторяемое чтение\"");
            Console.WriteLine("Использовать ISOLATION LEVEL REPEATABLE READ?");
            string yes = Console.ReadLine();
            bool use = yes.ToLower() == "yes" || yes.ToLower() == "да";

            using (var sConn1 = new NpgsqlConnection(sConnStr))
            {
                using (var sConn2 = new NpgsqlConnection(sConnStr))
                {
                    sConn1.Open();
                    sConn2.Open();
                    var sCommand = new NpgsqlCommand
                    {
                        Connection = sConn1,
                        CommandText = @"DROP TABLE IF EXISTS test;
                                        CREATE TABLE test
                                        (
                                            id  integer,
                                            num integer
                                        );

                                        INSERT INTO test (id, num)
                                        VALUES (1, 10);
                                        SELECT num FROM test;"
                    };
                    Console.WriteLine("Что храниться в таблице: " + sCommand.ExecuteScalar());

                    if (use)
                        sCommand = new NpgsqlCommand
                        {
                            Connection = sConn1,
                            CommandText = @"BEGIN ISOLATION LEVEL REPEATABLE READ ;
                                            SELECT num FROM test;"
                        };
                    else
                        sCommand = new NpgsqlCommand
                        {
                            Connection = sConn1,
                            CommandText = @"BEGIN;
                                            SELECT num FROM test;"
                        };
                    Console.WriteLine($"Первая транзакция считала (транзакция не завершилась): {sCommand.ExecuteScalar()}");

                    var sCommand2 = new NpgsqlCommand
                    {
                        Connection = sConn2,
                        CommandText = @"UPDATE test
                                        SET num = num + 200
                                        WHERE id = 1
                                        RETURNING num;"
                    };
                    Console.WriteLine($"Вторая транзакция обновила поле, новое значение: {sCommand2.ExecuteScalar()}");

                    sCommand = new NpgsqlCommand
                    {
                        Connection = sConn1,
                        CommandText = @"SELECT num FROM test;
                                        COMMIT;"
                    };
                    Console.WriteLine($"Первая транзакция завершилась, результат: {sCommand.ExecuteScalar()}");
                }

            }
            Console.WriteLine("Для выхода нажмите enter...");
            Console.ReadLine();
        }
    }
}
