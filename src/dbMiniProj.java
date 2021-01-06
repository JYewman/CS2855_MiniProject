import java.io.*;
import java.sql.*;
import java.util.Scanner;

class DB {
    private final String databaseURL;
    private Connection con;

    DB(String databaseURL) {
        this.databaseURL = databaseURL;
    }

    public boolean isConnected() {
        return (con != null);
    }

    public boolean auth(String username, String password) {
        try {
            con = DriverManager.getConnection(databaseURL, username, password);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    public void drop(String table) {
        try {
            Statement s = con.createStatement();
            s.execute("DROP TABLE IF EXISTS " + table);
            s.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void add(String tbName, String[][] colName) {
        try {
            StringBuilder s = new StringBuilder();
            for (int i = 0; i < colName.length; i++) {
                for (int n = 0; n < colName[i].length; n++) {
                    if (n != 0 || i != 0) {
                        s.append(" ");
                    }
                    s.append(colName[i][n]);
                }
                s.append(",");
            }
            s = new StringBuilder(s.substring(0, s.length() - 1));
            //System.out.println("CREATE TABLE " + tbName +" (" + s + ")");
            Statement statement = con.createStatement();
            statement.execute("CREATE TABLE " + tbName +" (" + s + ")");
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void addRow(String tableName, String[] colNames, String[] rowData){
        try{
            StringBuilder airportData = new StringBuilder();
            StringBuilder colName = new StringBuilder();
            for (int i = 0; i < rowData.length; i++){
                if(i != 0) {
                    airportData.append("', '");
                }
                airportData.append(rowData[i]);
            }
            for (int i = 0; i < colNames.length; i++) {
                if (i != 0) {
                    colName.append(", ");
                }
                colName.append(colNames[i]);
            }
            //System.out.println("INSERT INTO " + tableName + "(" + colName + ") VALUES ('" + airportData + "');");
            Statement statement = con.createStatement();
            statement.execute("INSERT INTO " + tableName + "(" + colName + ") VALUES ('" + airportData + "');");
            statement.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void addMultiRow(String tableName, String[] colNames, String rowData){
        try{
            StringBuilder data = new StringBuilder();
            StringBuilder col = new StringBuilder();
            for (int i = 0; i < colNames.length; i++){
                if (i != 0){
                    col.append(", ");
                }
                col.append(colNames[i]);
            }
            Statement statement = con.createStatement();
            statement.execute("INSERT INTO " + tableName +"(" + col + ") VALUES " + rowData + ";");
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}
class airportObject{
    String airportCode;
    String airportName;
    String city;
    String state;

    public airportObject(String airportCode, String airportName, String city, String state){
        this.airportCode = airportCode;
        this.airportName = airportName;
        this.city = city;
        this.state = state;
    }
}

class progress{
    static void newProgress(double progressComplete){
        System.out.print("\r[");
        for (int i = 0; i <= (int)(progressComplete); i++){
            System.out.print("=");
        }
        System.out.print("]");
    }
}
class dbMiniProj {

    public static void main(String[] args){
        DB database;
        Scanner usrInput = new Scanner(System.in);

        System.out.println("Enter Database URL (eg. teaching.cs.rhul.ac.uk/CS2855/<username>):");
        String dbAddressInput = usrInput.nextLine();
        if (dbAddressInput.equals("localhost")) {
            database = new DB("jdbc:postgresql://localhost/CS2855/zhac083");
            database.auth("zhac083",
                    "seeboh");
        }
        else {
            String dbAddr = ("jdbc:postgresql://" + dbAddressInput);
            System.out.println("Enter Username:");
            String user = usrInput.nextLine();
            System.out.println("Enter Password:");
            String password = usrInput.nextLine();

            database = new DB(dbAddr);
            if(!database.auth(user, password)) {
                System.out.println("Error! Check connection details.");
            }
        }
        if (!database.isConnected()) {
            usrInput.close();
        }
        if (database.isConnected()) {
            System.out.println("Connected to " + dbAddressInput);
            init(database);
            inject(database);
        }

    }

    public static void init(DB db) {
        System.out.println("Creating Database Tables...");
        db.drop("delayedFlights");
        db.drop("airports");

        String[][] airportCol = {
                {"airportCode","VARCHAR(3)","PRIMARY KEY"},
                {"airportName", "VARCHAR(100)"},
                {"City", "VARCHAR(100)"},
                {"State", "VARCHAR(5)"}
        };

        String[][] delFlightCol = {
                {"ID", "SERIAL", "PRIMARY KEY"},
                {"Month", "INT"},
                {"DayofMonth", "INT"},
                {"DayofWeek", "INT"},
                {"DepTime", "INT"},
                {"ScheduledDepTime", "INT"},
                {"ArrTime", "INT"},
                {"ScheduledArrTime", "INT"},
                {"UniqueCarrier", "VARCHAR(3)"},
                {"FlightNum", "INT"},
                {"ActualFlightTime", "INT"},
                {"scheduledFlightTime", "INT"},
                {"AirTime", "INT"},
                {"ArrDelay", "INT"},
                {"DepDelay", "INT"},
                {"Orig", "VARCHAR REFERENCES airports(airportCode)"},
                {"Dest", "VARCHAR REFERENCES airports(airportCode)"},
                {"Distance", "INT"}
        };

        db.add("airports", airportCol);
        db.add("delayedFlights", delFlightCol);
        System.out.println("OK!");
    }

    public static void inject(DB db){
        String airportPath = "airport.txt";
        String delayedFlightsPath = "delayedFlights.txt";
        String[] airportcol = {
                "airportCode",
                "airportName",
                "City",
                "State"
        };
        String[] flightcol = {
                "ID",
                "Month",
                "DayofMonth",
                "DayofWeek",
                "DepTime",
                "ScheduledDepTime",
                "ArrTime",
                "ScheduledArrTime",
                "UniqueCarrier",
                "FlightNum",
                "ActualFlightTime",
                "scheduledFlightTime",
                "AirTime",
                "ArrDelay",
                "DepDelay",
                "Orig",
                "Dest",
                "Distance"
        };

        System.out.println("Please Wait, Populating Airports...");
        try{
            FileInputStream fileStream = new FileInputStream(airportPath);
            DataInputStream dIn = new DataInputStream(fileStream);
            BufferedReader br = new BufferedReader(new InputStreamReader(dIn));
            String fLine;
            double progressComplete = 0.0;
            while ((fLine = br.readLine()) != null){
                String[] elements = fLine.split(",");
                db.addRow("airports", airportcol, elements);
                progressComplete += 0.05;
                progress.newProgress(progressComplete);
            }
        } catch (IOException e){
            e.printStackTrace();
        }

        System.out.println();
        System.out.println("Please Wait, Populating Flight Data...");
        try{
            FileInputStream fileStream = new FileInputStream(delayedFlightsPath);
            DataInputStream dIn = new DataInputStream(fileStream);
            BufferedReader br = new BufferedReader(new InputStreamReader(dIn));
            String fLine;
            StringBuilder s = new StringBuilder();
            while ((fLine = br.readLine()) != null){
                String[] elements = fLine.split(",");
                s.append("('");
                for (int i = 0; i <= elements.length - 1; i++){
                    if (i != 0){
                        s.append("', '");
                    }
                    s.append(elements[i]);
                }
                s.append("'), ");
            }
            String query = s.substring(0, s.length() - 2);
            //System.out.println(query);
            db.addMultiRow("delayedFlights", flightcol, query);
        } catch (IOException e){
            e.printStackTrace();
        }
    }
}