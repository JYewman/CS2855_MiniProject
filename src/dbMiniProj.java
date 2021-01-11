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

    public String[][] getResult(String sql) {
        try {
            Statement st = con.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY
            );
            if (st.execute(sql)) {
                return parseResultSet(st.getResultSet());
            } else {
                return null;
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            System.exit(0);
        }
        return null;
    }

    private String[][] parseResultSet(ResultSet resultSet) {
        try {
            int rows = 0;
            int columns = resultSet.getMetaData().getColumnCount();
            if (resultSet.last()) {
                rows = resultSet.getRow();
                resultSet.beforeFirst();
            }
            String[][] contents = new String[rows][columns];
            for (int j = 1; resultSet.next() && j <= rows; j++) {
                for (int i = 1; i <= columns; i++) {
                    contents[j - 1][i - 1] = resultSet.getString(i);
                }
            }
            resultSet.close();
            return contents;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return null;

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
            Statement statement = con.createStatement();
            statement.execute("CREATE TABLE " + tbName +" (" + s + ")");
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void addMultiRow(String tableName, String[] colNames, String rowData){
        try{
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

class dbMiniProj {
    public static int queryNum = 1;
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
            System.out.println("Connection Error! Check Address.");
            usrInput.close();
        }
        if (database.isConnected()) {
            System.out.println("Connected to " + dbAddressInput);
            init(database);
            inject(database);
            query(database);
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
            db.addMultiRow("airports", airportcol, query);
        } catch (IOException e){
            e.printStackTrace();
        }

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
            db.addMultiRow("delayedFlights", flightcol, query);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void output(String[][] data){
        System.out.print("################## " + queryNum);
        if (queryNum == 1){
            System.out.print("st");
        } else if (queryNum == 2){
            System.out.print("nd");
        } else if (queryNum == 3){
            System.out.print("rd");
        } else {
            System.out.print("th");
        }
        System.out.println(" Query ################");
        queryNum++;
        for (String[] datum : data) {
            for (String s : datum) {
                System.out.print(s);
                System.out.print(" ");
            }
            System.out.println();
        }
    }

    public static void query(DB db){
        String[][] queryDB = db.getResult("SELECT UniqueCarrier, count(*) from delayedFlights " +
                "GROUP BY UniqueCarrier ORDER BY count(*) DESC LIMIT 5;");
        output(queryDB);
        queryDB = db.getResult("SELECT a.City, count(d.Orig) from airports a INNER JOIN delayedFlights d ON " +
                "a.airportcode = d.Orig GROUP BY a.City ORDER BY count(d.Orig) DESC LIMIT 5;");
        output(queryDB);
        queryDB = db.getResult("SELECT Dest, SUM(ArrDelay) FROM delayedFlights GROUP BY dest " +
                "ORDER BY SUM(ArrDelay) DESC LIMIT 5 OFFSET 1;");
        output(queryDB);
    }

}