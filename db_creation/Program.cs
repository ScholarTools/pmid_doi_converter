using System;
using System.IO;
using System.IO.Compression;
using System.Diagnostics;
using System.Xml;
using System.Xml.Linq;
using System.Collections.Generic;
using System.Xml.XPath;

using MySql.Data;
using MySql.Data.MySqlClient;

//CD to directory in terminal
//run   dotnet new console
//dotnet run

namespace db_creation
{
    class Program
    {
        static void Main(string[] args)
        {

            var user = Environment.GetEnvironmentVariable("mysql_user");
            var pass = Environment.GetEnvironmentVariable("mysql_pass");
            string connStr = "server=localhost;user=" + user + ";database=mydb;port=3306;password=" + pass;
            MySqlConnection conn = new MySqlConnection(connStr);
            conn.Open();

            using var cmd = new MySqlCommand();
            cmd.Connection = conn;
            cmd.CommandText = "DROP TABLE IF EXISTS ids";
            cmd.ExecuteNonQuery();

            cmd.CommandText = @"CREATE TABLE ids( 
                              id INT AUTO_INCREMENT PRIMARY KEY, 
                              pmid INT NOT NULL UNIQUE,
                              doi VARCHAR(255), INDEX(doi))
                              CHARACTER SET utf8mb4";
            cmd.ExecuteNonQuery();

            string rootFolder = "/Users/jim/Desktop/pubmed/";
            string filePath = rootFolder + "pubmed21n1091.xml.gz";

            string [] fileEntries = Directory.GetFiles(rootFolder,"*.gz");
            Array.Sort(fileEntries);

            //foreach (string filePath in fileEntries){

                var stopwatch = new Stopwatch();
                stopwatch.Start();         

                Console.WriteLine(filePath);
                AddFileToDB(cmd,filePath);

                stopwatch.Stop();
                var elapsed_time = stopwatch.ElapsedMilliseconds;

                Console.WriteLine("Elapsed (ms): {0:N}", elapsed_time);
            //}
        }

         public static void AddFileToDB(MySqlCommand cmd, string file_path){


            MySqlTransaction trans = cmd.Connection.BeginTransaction();
            cmd.Transaction = trans;  

            var fileToDecompress = new FileInfo(file_path);
            var outputStream = new MemoryStream();

            using (FileStream originalFileStream = fileToDecompress.OpenRead())
            {
                using (GZipStream decompressionStream = new GZipStream(
                        originalFileStream, CompressionMode.Decompress))
                {
                    //https://stackoverflow.com/questions/35720959/decompressing-gzip-stream
                    decompressionStream.CopyTo(outputStream);
                    outputStream.Position = 0;


                    XDocument doc = XDocument.Load(outputStream);
                    XElement root = doc.Root;

                    string pmid;
                    string doi;
                    XElement x_pmid;
                    var i = 0;
                    var count = 0;
                    foreach (var x_article in root.Elements()){
                        i = i + 1;
                        try{
                            XElement x_id_list = x_article.Element("PubmedData").Element("ArticleIdList");

                            //XName.Get("Name", @"http://demo.com/2011/demo-schema")
                            //XElement x_doi = x_id_list.Element("ArticleId[@IdType=\"doi\"]");

                            XElement x_doi = x_id_list.XPathSelectElement("ArticleId[@IdType=\"doi\"]");
                            x_pmid = x_id_list.XPathSelectElement("ArticleId[@IdType=\"pubmed\"]");
                            pmid = x_pmid.Value;
                            doi = x_doi.Value;
                            count = count + 1;
                        }catch{
                            x_pmid = x_article.Element("MedlineCitation").Element("PMID");
                            pmid = x_pmid.Value;
                            doi = "";
                        }

                        cmd.CommandText = String.Format(
                            "INSERT INTO ids(pmid,doi) VALUES({0:D},'{1}') ON DUPLICATE KEY UPDATE doi='{1}'",
                            pmid,doi);
                        cmd.ExecuteNonQuery();

                        //nodeList=root.SelectNodes("descendant::book[author/last-name='Austen']");

                        //This always exists ...
                        

                        //t.Add(Convert.ToInt32(pmid));
                        
                    }


                    cmd.Transaction.Commit();

                    Console.WriteLine("DOI count {0:N}",count);
                    Console.WriteLine("# children: {0:N}",i);
                    //Console.WriteLine("200th: {0:N}",t[200]);


                }
            }







         }
    }
}
