
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.mindrot.jbcrypt.BCrypt;

import java.io.*;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

class main {
    private static String username = "";
    private static PreparedStatement registerStatement;
    private static PreparedStatement checkUsernameStatement;
    private static PreparedStatement updateWalletStatement;
    private static PreparedStatement viewCastStatement;
    private static PreparedStatement viewGenreStatement;
    private static PreparedStatement viewMoviesStatement;
    private static PreparedStatement filterGenreStatement;
    private static PreparedStatement filterCastStatement;
    private static PreparedStatement checkMovieStatement;
    private static PreparedStatement updateUserMoviesStatement;
    private static PreparedStatement checkUserMoviesStatement;


    public static void main(String[] args) throws SQLException {
        Connection connection = connect();
        if (connection == null) {
            return;
        }
//        populate(connection);
//        populateCast(connection);
//        populateGenre(connection);

        prepareStatements(connection);
        if (opening(connection)) {
            while (menu(connection)) {
            }
        } else {
            System.out.println("Error occurred");
        }
        connection.close();
    }


    public static boolean opening(Connection connection) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please login or register:");
        System.out.println(" *** Please enter one of the following commands *** ");
        System.out.println("> create <username> <password> <initial amount>");
        System.out.println("> login <username> <password>");
        while (true) {
            String[] line = scanner.nextLine().split(" ");
            if (line[0].equals("create")) {
                if (line.length != 4) {
                    System.out.println("Invalid command");
                }
                String username = line[1];
                String password = line[2];
                try {
                    double amount = Double.parseDouble(line[3]);
                    try {
                        if (register(username, password, amount, connection))  {
                            System.out.println("User created.\nLogged into user\n");
                            return true;
                        }
                    } catch (SQLException ignored) {
                        return false;
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Invalid initial amount");
                }
            } else if (line[0].equals("login")) {
                if (line.length != 3) {
                    System.out.println("Invalid command");
                }
                String username = line[1];
                String password = line[2];
                if (login(username, password)) {
                    System.out.println("Login successful.");
                    return true;
                }
            } else {
                System.out.println("Invalid command");
            }
        }
    }

    public static boolean menu(Connection connection) throws SQLException {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println();
            System.out.println(" *** Please enter one of the following commands *** ");
            System.out.println("> view cast");
            System.out.println("> view genre");
            System.out.println("> view movies");
            System.out.println("> view my info");
            System.out.println("> filter genre <name>");
            System.out.println("> filter cast <name>");
            System.out.println("> buy movie <movieId>");
            System.out.println("> add money <amount>");
            System.out.println("> quit");

            String line = scanner.nextLine();
            String[] segmented = line.split(" ");
            if (segmented.length == 0) {
                return true;
            }
            if (line.equals("quit")) {
                return false;
            } else if (line.equals("view my info")) {
                checkUserInfo();
            } else if (segmented[0].equals("view")) {
                if (segmented.length != 2) {
                    System.out.println("Invalid amount of arguments");
                } else if (segmented[1].equals("cast")) {
                    listAll(viewCastStatement.executeQuery());
                } else if (segmented[1].equals("genre")) {
                    listAll(viewGenreStatement.executeQuery());
                } else if (segmented[1].equals("movies")) {
                    listAll(viewMoviesStatement.executeQuery());
                } else {
                    System.out.println("Invalid view argument");
                }
            } else if (segmented[0].equals("filter")) {
                if (segmented[1].equals("genre")) {
                    filterGenreStatement.clearParameters();
                    filterGenreStatement.setString(1, segmented[2]);
                    listAll(filterGenreStatement.executeQuery());
                } else if (segmented[1].equals("cast")) {
                    String name = line.split("filter cast")[1].trim();
                    filterCastStatement.clearParameters();
                    filterCastStatement.setString(1, name);
                    listAll(filterCastStatement.executeQuery());
                } else {
                    System.out.println("Invalid filter argument");
                }
                System.out.println("");
            } else if (line.startsWith("buy movie ")) {
                try {
                    int movieId = Integer.parseInt(line.split("buy movie ")[1]);
                    System.out.println(movieId);
                    if (buyMovie(movieId, connection)) {
                        System.out.println("Bought movie.");
                    } else {
                        System.out.println("Error occurred");
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Argument isn't a valid number");
                }
            } else if (line.startsWith("add money ")) {
                try {
                    double amount = Double.parseDouble(line.split("add money ")[1]);
                    if (amount <= 0) {
                        System.out.println("Invalid amount");
                        continue;
                    }
                    updateWallet(connection, amount);
                } catch (NumberFormatException e) {
                    System.out.println("Argument isn't a valid number");
                }
            } else {
                System.out.println("Invalid command");
            }
            return true;
        }
    }

    // Connects to the database
    public static Connection connect() {
        try {
            Scanner scanner = new Scanner(new File("credentials.txt"));
            String jdbcURL = "jdbc:mysql://localhost:3306/movies";
            String username = scanner.nextLine().split("username: ")[1];
            String password = scanner.nextLine().split("password: ")[1];
            Connection connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            connection.setAutoCommit(false);
            return connection;
        } catch (FileNotFoundException | SQLException e) {
            System.out.println("Credentials are invalid/not found");
            System.err.println(e);
        }
        return null;
    }

    // Populates the tables
    public static void populate(Connection connection) {
        try {
            connection.setAutoCommit(false);
            String sql = "INSERT INTO Movies (movie_id, budget, genres, homepage, keywords, original_language, original_title," +
                    "overview, popularity, production_companies, production_countries, release_date, revenue, runtime," +
                    "spoken_languages, status, tagline, title, vote_average, vote_count) VALUE (?, ?, ?, ?, ?, ?, ?, ?" +
                    ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            String file = "tmdb_5000_movies.csv";
            int index = 0;
            String[] data = null;
            CSVReader csvReader = new CSVReader(new FileReader(file));
            csvReader.readNext();
            while((data = csvReader.readNext()) != null) {
                int id;
                String genres = data[1];
                String homepage = data[2];
                int budget;
                String keywords = data[4];
                String original_language = data[5];
                String original_title = data[6];
                String overview = data[7];
                float popularity;
                String production_companies = data[9];
                String production_countries = data[10];

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                Date release_date = null;
                try {
                    release_date = new Date(sdf.parse(data[11]).getTime());
                } catch (java.text.ParseException ignored) {
                    System.out.println("ignored");
                }
                double revenue;
                double runtime;
                String spoken_languages = data[14];
                String status = data[15];
                String tagline = data[16];
                String title = data[17];
                double vote_average;
                int vote_count;

                try {
                    budget = Integer.parseInt(data[0]);
                    id = Integer.parseInt(data[3]);
                    popularity = Float.parseFloat(data[8]);
                    revenue = Double.parseDouble(data[12]);
                    runtime = Double.parseDouble(data[13]);
                    vote_average = Double.parseDouble(data[18]);
                    vote_count = Integer.parseInt(data[19]);
                } catch (NumberFormatException e) {
                    continue;
                }
                statement.setInt(1, id);
                statement.setInt(2, budget);
                statement.setString(3, genres);
                statement.setString(4, homepage);
                statement.setString(5, keywords);
                statement.setString(6, original_language);
                statement.setString(7, original_title);
                statement.setString(8, overview);
                statement.setFloat(9, popularity);
                statement.setString(10, production_companies);
                statement.setString(11, production_countries);
                if (release_date == null) {
                    statement.setNull(12, Types.DATE);
                } else {
                    statement.setDate(12, release_date);
                }
                statement.setDouble(13, revenue);
                statement.setDouble(14, runtime);
                statement.setString(15, spoken_languages);
                statement.setString(16, status);
                statement.setString(17, tagline);
                statement.setString(18, title);
                statement.setDouble(19, vote_average);
                statement.setInt(20, vote_count);

                statement.executeUpdate();
            }

            csvReader.close();

            sql = "INSERT INTO Credits (movie_id, title, cast, crew) VALUES (?, ?, ?, ?)";
            statement = connection.prepareStatement(sql);

            file = "tmdb_5000_credits.csv";
            csvReader = new CSVReader(new FileReader(file));
            csvReader.readNext();
            while((data = csvReader.readNext()) != null) {
                System.out.println(index);
                System.out.println(Arrays.toString(data));
                int movie_id;
                try {
                    movie_id = Integer.parseInt(data[0]);
                } catch (NumberFormatException e) {
                    continue;
                }
                String title = data[1];

                String cast = data[2].replaceAll("\"\"[^\"]*\"\"", "");
                String crew = data[3].replaceAll("\"\"[^\"]*\"\"", "");
                // In case the JSON data in the csv is incomplete
                if (cast.length() == 0) {
                    cast = "[]";
                } else if (cast.charAt(cast.length() - 1) != ']') {
                    System.out.println(cast);
                    System.out.println(cast.charAt(cast.length() - 1));
                    for (int a = cast.length() - 1; a > 0; a--) {
                        if (cast.charAt(a) == '{') {
                            if (a - 2 < 0) {
                                continue;
                            }
                            cast = cast.substring(0, a - 2) + "]";
                            break;
                        }
                    }
                }
                if (crew.length() == 0) {
                    crew = "[]";
                } else if (crew.charAt(crew.length() - 1) != ']') {
                    for (int a = crew.length() - 1; a > 0; a--) {
                        if (crew.charAt(a) == '{') {
                            if (a - 2 < 0) {
                                continue;
                            }
                            crew = crew.substring(0, a - 2) + "]";
                            break;
                        }
                    }
                }

                statement.setInt(1, movie_id);
                statement.setString(2, title);
                statement.setString(3, cast);
                statement.setString(4, crew);
                try {
                    statement.executeUpdate();
                } catch (Exception ignored) {
                }

            }
            csvReader.close();

            connection.commit();
//            connection.close();

        } catch (IOException | CsvValidationException | NumberFormatException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static void populateGenre(Connection connection) {
        String file = "tmdb_5000_movies.csv";
        String[] data = null;
        try {
            CSVReader csvReader = new CSVReader(new FileReader(file));
            csvReader.readNext();
            // <id>, <genre name>
            Map<Integer, String> map = new TreeMap<Integer, String>();
            while((data = csvReader.readNext()) != null) {
                String[] genre = data[1].substring(1, data[1].length() - 1).split("}, ");
                for (String element: genre) {
                    if (element.length() == 0) {
                        continue;
                    }
                    if (element.charAt(element.length() - 1) != '}') {
                        element += '}';
                    }
                    JSONParser parser = new JSONParser();
                    JSONObject json = (JSONObject) parser.parse(element);
                    Integer id = ((Long) json.get("id")).intValue();

                    String genreName = (String) json.get("name");
                    if (!map.containsKey(id)) {
                        map.put(id, genreName);
                    }
                }

            }
            String sql = "INSERT INTO Genre (genreId, genreName) VALUE (?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            for (int id: map.keySet()) {
                statement.clearParameters();
                statement.setInt(1, id);
                statement.setString(2, map.get(id));
                statement.executeUpdate();
            }
            connection.commit();

        } catch (IOException | CsvValidationException | ParseException | SQLException ignored) {
        }
    }

    public static void populateCast(Connection connection) {
        String file = "tmdb_5000_credits.csv";
        String[] data = null;
        try {
            CSVReader csvReader = new CSVReader(new FileReader(file));
            csvReader.readNext();
            // <id>, <name>
            Map<Integer, String> map = new TreeMap<Integer, String>();
            while((data = csvReader.readNext()) != null) {
                if (data[2].length() == 0) {
                    continue;
                }
                String[] cast = data[2].substring(1, data[2].length() - 1).split("}, ");
                for (String element: cast) {
                    if (element.length() == 0) {
                        continue;
                    }
                    if (element.charAt(element.length() - 1) != '}') {
                        element += '}';
                    }
                    try {
                        JSONParser parser = new JSONParser();
                        System.out.println(element);
                        JSONObject json = (JSONObject) parser.parse(element);
                        Integer id = ((Long) json.get("cast_id")).intValue();

                        String genreName = (String) json.get("name");
                        if (!map.containsKey(id)) {
                            map.put(id, genreName);
                        }
                    } catch (Exception ignored) {
                    }
                }
            }
            String sql = "INSERT INTO Cast (castId, name) VALUE (?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            for (int id: map.keySet()) {
                statement.clearParameters();
                statement.setInt(1, id);
                statement.setString(2, map.get(id));
                statement.executeUpdate();
            }
            connection.commit();

        } catch (IOException | CsvValidationException | SQLException ignored) {
        }
    }

    public static String hashPassword(String password) {
        int workload = 11;
        return BCrypt.hashpw(password, BCrypt.gensalt(workload));
    }

    public static boolean checkPassword(String password, String storedPassword) {
        if (null == storedPassword || !storedPassword.startsWith("$2a$"))
            return false;

        return BCrypt.checkpw(password, storedPassword);
    }

    public static void prepareStatements(Connection connection) throws SQLException {
        String checkUsernameSQL = "SELECT * FROM users WHERE username = ?";
        checkUsernameStatement = connection.prepareStatement(checkUsernameSQL);

        String registerSQL = "INSERT INTO Users (username, password, wallet_balance, movies) VALUE (?, ?, ?, ?)";
        registerStatement = connection.prepareStatement(registerSQL);

        String updateWallet = "UPDATE Users SET wallet_balance = ? WHERE username = ?";
        updateWalletStatement = connection.prepareStatement(updateWallet);

        String viewCastSQL = "SELECT name FROM CAST";
        viewCastStatement = connection.prepareStatement(viewCastSQL);

        String viewGenreSQL = "SELECT genreName FROM GENRE";
        viewGenreStatement = connection.prepareStatement(viewGenreSQL);

        String viewMovieSQL = "SELECT * FROM MOVIES";
        viewMoviesStatement = connection.prepareStatement(viewMovieSQL);

        String filterGenreSQL = "SELECT * FROM MOVIES WHERE JSON_CONTAINS(genres, JSON_OBJECT('name', ?));";
        filterGenreStatement = connection.prepareStatement(filterGenreSQL);

        String filterCastSQL = "SELECT * FROM MOVIES M, CREDITS C WHERE M.movie_id = C.movie_id " +
                "AND JSON_CONTAINS(C.cast, JSON_OBJECT('name', ?))";
        filterCastStatement = connection.prepareStatement(filterCastSQL);

        String checkMovieSQL = "SELECT * FROM MOVIES WHERE movie_id = ?";
        checkMovieStatement = connection.prepareStatement(checkMovieSQL);

        String updateUserMoviesSQL = "UPDATE USERS SET movies = JSON_ARRAY_APPEND(movies, '$', CAST(? AS JSON)) WHERE username = ?";
        updateUserMoviesStatement = connection.prepareStatement(updateUserMoviesSQL);

        String checkUserMoviesSQL = "SELECT * FROM USERS WHERE username = ? AND  JSON_CONTAINS(movies, JSON_OBJECT('name', ?))";
        checkUserMoviesStatement = connection.prepareStatement(checkUserMoviesSQL);
        
    }

    public static boolean login(String user, String password) {
        try {
            checkUsernameStatement.clearParameters();
            checkUsernameStatement.setString(1, user);

            ResultSet results = checkUsernameStatement.executeQuery();

            if (!results.next()) {
                results.close();
                System.out.println("Login failed");
                return false;
            }
            String dbPassword = results.getString("password");
            results.close();

            if (checkPassword(password, dbPassword)) {
                username = user;
                return true;
            } else {
                System.out.println("Invalid password");
                return false;
            }
        } catch (SQLException e) {
            System.out.println("Login SQL failed");
        }
        System.out.println("Login failed");
        return false;
    }

    public static boolean register(String user, String password, Double wallet_balance, Connection connection)
            throws SQLException {
        try {
            checkUsernameStatement.clearParameters();
            checkUsernameStatement.setString(1, user);
            ResultSet results = checkUsernameStatement.executeQuery();
            if (results.next()) {
                results.close();
                System.out.println("Username already exists");
                connection.rollback();
                return false;
            } else {
                results.close();
                registerStatement.clearParameters();
                registerStatement.setString(1, user);
                registerStatement.setString(2, hashPassword(password));
                registerStatement.setDouble(3, wallet_balance);
                registerStatement.setString(4, "[]");
                registerStatement.executeUpdate();
            }
            username = user;
            connection.commit();
            return true;
        } catch (SQLException e) {
            System.out.println("Register SQL failed");
        }
        connection.rollback();
        return false;
    }

    public static boolean updateWallet(Connection connection, double amount) throws SQLException {
        checkUsernameStatement.clearParameters();
        checkUsernameStatement.setString(1, username);

        ResultSet results = checkUsernameStatement.executeQuery();
        results.next();
        double balance = results.getDouble("wallet_balance");
        results.close();

        if (balance + amount >= 0) {
            updateWalletStatement.clearParameters();
            updateWalletStatement.setDouble(1, balance + amount);
            updateWalletStatement.setString(2, username);
            updateWalletStatement.executeUpdate();
            connection.commit();
            return true;
        } else {
            System.out.println("Not enough money in wallet");
        }
        connection.rollback();
        return false;
    }

    public static boolean buyMovie(int movieId, Connection connection) throws SQLException {
        // Check if movie exists
        // Check if user has movie
        // Check if user has enough money - already handled by update wallet
        // Update user movie
        checkMovieStatement.clearParameters();
        checkMovieStatement.setInt(1, movieId);
        ResultSet set = checkMovieStatement.executeQuery();
        if (!set.next()) {
            set.close();
            System.out.println("Movie doesn't exist");
            connection.rollback();
            return false;
        }
        String json = "{\"id\": ";
        String name = set.getString("title");
        json += movieId + ", \"name\": \"" + name + "\"}";
        set.close();

        checkUserMoviesStatement.clearParameters();
        checkUserMoviesStatement.setString(1, username);
        checkUserMoviesStatement.setString(2, name);
        set = checkUserMoviesStatement.executeQuery();
        if (set.next()) {
            set.close();
            System.out.println("You already have this movie");
            connection.rollback();
            return false;
        }
        if (!updateWallet(connection, -10)) {
            connection.rollback();
            return false;
        }
        updateUserMoviesStatement.clearParameters();
        updateUserMoviesStatement.setString(1, json);
        updateUserMoviesStatement.setString(2, username);
        updateUserMoviesStatement.executeUpdate();
        connection.commit();
        return true;
    }

    public static boolean checkUserInfo() {
        try {
            checkUsernameStatement.clearParameters();
            checkUsernameStatement.setString(1, username);
            ResultSet set = checkUsernameStatement.executeQuery();
            set.next();
            double wallet_balance = set.getDouble("wallet_balance");
            String movies = set.getString("movies");
            System.out.println("My wallet balance: " + wallet_balance);
            System.out.println("My movies: " + movies + "\n");
            return true;
        } catch (SQLException e) {
            return false;
        }
    }


    public static void listAll(ResultSet set) {
        try {
            ResultSetMetaData rsmd = set.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (set.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = set.getString(i);
                    System.out.print(rsmd.getColumnName(i) + ": " + columnValue);
                }
                System.out.println("");
            }
        } catch (SQLException ignored) {
        }
    }

}
