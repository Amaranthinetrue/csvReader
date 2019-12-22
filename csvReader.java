package net.csvReader;

import java.io.*;
import java.sql.*;
import java.sql.DriverManager;
import java.sql.Connection;

public class csvReader
{
    public static void main(String[] args) throws FileNotFoundException, IOException, SQLException
    {
        BufferedReader br = null;

        String line = "";
        String splitby = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
        String csvFileToRead = args[0];
        String fileName = csvFileToRead;
        String[] baseFileName = fileName.split(".c");
        String badcsv = baseFileName[0] + "-bad.csv";
        String logFile = baseFileName[0] + ".log";
        String url = "jdbc:sqlite:" + baseFileName[0] + ".db";
        String makeTable = "CREATE TABLE IF NOT EXISTS sample (\n";
        String insert = "INSERT INTO sample(";
        String Values = "VALUES(";

        File file = new File(csvFileToRead);

        br = new BufferedReader(new FileReader(file));

        int total = 0;
        int bad = 0;
        int good = 0;

        PrintWriter out = new PrintWriter(badcsv);
        PrintWriter log = new PrintWriter(logFile);

        try(Connection conn = DriverManager.getConnection(url)) 
        {
            if (conn != null) 
            {
                Statement stmt = conn.createStatement();
                //read first line of file for headers
                // create a new table
                line = br.readLine();
                String[] headers = line.split(splitby,-1);
                for(String element : headers)
                    {
                        makeTable += element + " text,\n";
                        insert += element + ",";
                        Values += "?,";
                    }
                makeTable = makeTable.substring(0,(makeTable.length())-2);
                makeTable += "\n);";
                insert = insert.substring(0,(insert.length())-1);
                Values = Values.substring(0,(Values.length())-1);
                insert += ") " + Values + ")";
                stmt.execute(makeTable);
                while((line = br.readLine()) !=null)
                {
                    String[] products = line.split(splitby,-1);
                    ++total;
                    if(products.length != headers.length)
                    {
                        out.println(line);
                        ++bad;
                    }
                    else
                    {
                        PreparedStatement pstmt = conn.prepareStatement(insert);
                        int I=1;
                        for (String element : products)
                        {
                            pstmt.setString(I,element);
                            I++;
                        }
                        pstmt.executeUpdate();
                        ++good;
                    }
                }
            }
 
        } catch (SQLException e) 
        {
            System.out.println(e.getMessage());
        }
        log.printf("total imput = %d\n", total);
        log.printf("total sucessful records = %d\n", good);
        log.printf("total failed records = %d\n", bad);
        log.close();
        out.close();
    }
}