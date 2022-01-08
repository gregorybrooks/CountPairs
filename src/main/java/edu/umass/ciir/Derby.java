package edu.umass.ciir;
import java.sql.*;
import java.util.Objects;
//import org.apache.derby.jdbc.EmbeddedDriver;

public class Derby {
    enum StatTypes { counts_unordered_gap, counts_ordered_gap, counts_unordered_inwindow, count_indoc,
        document_frequencies, collection_frequencies, total_collection_term_instances, total_documents}

    class TermStat {
        String term;
        String type;
        TermStat(String term, String type) {
            this.term = term;
            this.type = type;
        }
        String getType() {
            return type;
        }
        String getKey() {
            return type + term;
        }
        String getTerm() {
            return term;
        }
    }

    class Pair {
        String first_term;
        String second_term;

        public Pair(String first_term, String second_term) {
            this.first_term = first_term;
            this.second_term = second_term;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pair pair = (Pair) o;
            return first_term.equals(pair.first_term) && second_term.equals(pair.second_term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(first_term, second_term);
        }
    }

    class PairStatKey {
        Pair pair;
        String type;
        int index;

        public PairStatKey(Pair pair, String type, int index) {
            this.pair = pair;
            this.type = type;
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PairStatKey that = (PairStatKey) o;
            return index == that.index && pair.equals(that.pair) && type.equals(that.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pair, type, index);
        }
    }

    class PairStat {
        String first_term;
        String second_term;
        int index;
        PairStat(String first_term, String second_term) {
            this.first_term = first_term;
            this.second_term = second_term;
            this.index = -1;
        }
        PairStat(String first_term, String second_term, int index) {
            this.first_term = first_term;
            this.second_term = second_term;
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PairStat pairStat = (PairStat) o;
            return index == pairStat.index && first_term.equals(pairStat.first_term) && second_term.equals(pairStat.second_term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(first_term, second_term, index);
        }
    }

    Connection conn = null;
    Statement pairStmt;
    ResultSet pairRs = null;
    Statement featureStmt;
    ResultSet featureRs = null;
    Statement termStmt;
    ResultSet termRs = null;
    int rowCount = 0;
    PreparedStatement ps;

    public void connect() {
        try {
            conn = DriverManager.getConnection
                    ("jdbc:derby:/u02/derby_data");
            conn.setAutoCommit(false);



        } catch (SQLException ex) {
            System.out.println("in connection" + ex);
        }
    }

    public void createTable() {
        try {
            Statement stmt = conn.createStatement();
            String dropSQL = "drop table global_stats";
            try {
                stmt.execute(dropSQL);
                conn.commit();
            } catch (SQLException e) {
                ;
            }

            dropSQL = "drop table counts_ordered_gap";
            try {
                stmt.execute(dropSQL);
                conn.commit();
            } catch (SQLException e) {
                ;
            }

            dropSQL = "drop table term_stats";
            try {
                stmt.execute(dropSQL);
                conn.commit();
            } catch (SQLException e) {
                ;
            }

            dropSQL = "drop table features";
            try {
                stmt.execute(dropSQL);
                conn.commit();
            } catch (SQLException e) {
                ;
            }

            dropSQL = "drop table frequencies";
            try {
                stmt.execute(dropSQL);
                conn.commit();
            } catch (SQLException e) {
                ;
            }

            String createSQL = "create table counts_ordered_gap (first_term varchar(100), second_term varchar(100), " +
                "           index int) ";

            stmt.execute(createSQL);
            ps = conn.prepareStatement(
                    "insert into counts_ordered_gap (first_term, second_term, index) values (?,?,?)");
            conn.commit();

            createSQL = "create table term_stats (term varchar(100), collection_frequency int default 0, document_frequency int default 0, primary key (term))";
            stmt.execute(createSQL);
            conn.commit();

            createSQL = "create table frequencies (term varchar(100), type varchar(10), frequency int default 0, primary key (term, type))";
            stmt.execute(createSQL);
            conn.commit();

            createSQL = "create table global_stats (total_collection_term_instances int default 0, total_documents int default 0)";
            stmt.execute(createSQL);
            String statement = "insert into global_stats (total_collection_term_instances, total_documents) values (0, 0)";
            stmt.execute(statement);
            conn.commit();

            createSQL = "create table features (first_term varchar(100), second_term varchar(100), " +
                    "           count_indoc double, " +
                    "           counts_unordered_gap0 double,  " +
                    "           counts_unordered_gap1 double,  " +
                    "           counts_unordered_gap2 double, " +
                    "           counts_unordered_gap3 double," +
                    "           counts_unordered_gap4 double," +
                    "           counts_unordered_gap5 double," +
                    "           counts_ordered_gap0 double,  " +
                    "           counts_ordered_gap1 double,  " +
                    "           counts_ordered_gap2 double, " +
                    "           counts_ordered_gap3 double," +
                    "           counts_ordered_gap4 double," +
                    "           counts_ordered_gap5 double," +
                    "           counts_unordered_inwindow0 double,  " +
                    "           counts_unordered_inwindow1 double,  " +
                    "           counts_unordered_inwindow2 double, " +
                    "           counts_unordered_inwindow3 double," +
                    "           counts_unordered_inwindow4 double," +
                    "           counts_unordered_inwindow5 double," +
                       "primary key(first_term, second_term))";

            stmt.execute(createSQL);
            conn.commit();

        } catch (SQLException ex) {
            throw new AppException("in connection" + ex);
        }
    }

    public void closeDatabase () {
        try {
            conn.commit();
            DriverManager.getConnection
                    ("jdbc:derby:;shutdown=true");
        } catch (SQLException ex) {
            if (((ex.getErrorCode() == 50000) &&
                    ("XJ015".equals(ex.getSQLState())))) {
                //throw new AppException("Derby shut down normally");
            } else {
                throw new AppException("Derby did not shut down normally: " + ex.getMessage());
            }
        }
    }

    public void bumpPairStat(String first, String second, int index, StatTypes statType) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from pair_stats where first_term = '" + first + "' and second_term = '" + second + "'");
            if (!rs.next()) {
                String statement = "insert into pair_stats (first_term, second_term) values ('" + first + "', '" + second + "')";
                stmt.execute(statement);
            }
            String statement;
            if (statType == StatTypes.count_indoc) {
                statement = "update pair_stats set count_indoc = count_indoc + 1 where first_term = '" + first + "' and second_term = '" + second + "'";
            } else if (statType == StatTypes.counts_ordered_gap || statType == StatTypes.counts_unordered_gap
                   || statType == StatTypes.counts_unordered_inwindow) {
                String statName = statType.toString() + index;
                statement = "update pair_stats set " + statName + " = " + statName + " + 1 where first_term = '" + first + "' and second_term = '" + second + "'";
            } else {
                throw new AppException("Invalid stat type: " + statType.toString());
            }
            stmt.execute(statement);
            if (++rowCount > 10000) {
                conn.commit();
            }
        } catch (SQLException ex) {
            throw new AppException("in connection" + ex);
        }
    }

    public void bumpTermStat(String term, StatTypes statType) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from term_stats where term = '" + term + "'");
            if (!rs.next()) {
                int collection_value = 0;
                int document_value = 0;
                if (statType == StatTypes.collection_frequencies) {
                    collection_value = 1;
                } else if (statType == StatTypes.document_frequencies){
                    document_value = 1;
                } else {
                    throw new AppException("Invalid stat type: " + statType.toString());
                }
                String statement = "insert into term_stats (term, collection_frequency, document_frequency) values ('" + term + "', "
                        + collection_value + ", " + document_value + ")";
                stmt.execute(statement);
            } else {
                String statement;
                if (statType == StatTypes.document_frequencies) {
                    statement = "update term_stats set document_frequency = document_frequency + 1 " + "where term = '" + term + "'";
                } else if (statType == StatTypes.collection_frequencies) {
                    statement = "update term_stats set collection_frequency = collection_frequency + 1 " + "where term = '" + term + "'";
                } else {
                    throw new AppException("Invalid stat type: " + statType.toString());
                }
                stmt.execute(statement);
            }
            if (++rowCount > 10000) {
                conn.commit();
            }
        } catch (SQLException ex) {
            throw new AppException("in connection" + ex);
        }
    }

    public void addFrequencyStat(String term, Long count, StatTypes type) {
        try {
            Statement stmt = conn.createStatement();
            String statement;
            if (type == StatTypes.collection_frequencies) {
                statement = "insert into frequencies (term, type, frequency) values ('" + term + "', 'collection', "
                        + count + ")";
            } else if (type == StatTypes.document_frequencies) {
                statement = "insert into frequencies (term, type, frequency) values ('" + term + "', 'document', "
                        + count + ")";
            } else {
                throw new AppException("invalid stat type");
            }
            stmt.execute(statement);
        } catch (SQLException ex) {
            throw new AppException("in connection" + ex);
        }
    }

    public void addPairStat(PairStat pairStat) {
        try {
            ps.setString(1, pairStat.first_term);
            ps.setString(2, pairStat.second_term);
            ps.setInt(3, pairStat.index);
            ps.execute();
            if (++rowCount % 5000 == 0) {
//                System.out.println("At " + rowCount);
                conn.commit();
            }
        } catch (SQLException ex) {
            throw new AppException("in connection" + ex);
        }
    }

    public void setGlobalStat(StatTypes statType, int value) {
        try {
            Statement stmt = conn.createStatement();
            String statement;
            if (statType == StatTypes.total_collection_term_instances) {
                statement = "update global_stats set total_collection_term_instances = " + value;
            } else if (statType == StatTypes.total_documents){
                statement = "update global_stats set total_documents = " + value;
            } else {
                throw new AppException("Invalid stat type: " + statType.toString());
            }
            stmt.execute(statement);
        } catch (SQLException ex) {
            throw new AppException("in connection" + ex);
        }
    }

    public Stats getFirstPairStat() {
        try {
            pairStmt = conn.createStatement();
            pairRs = pairStmt.executeQuery("select * from pair_stats ");
            if (!pairRs.next()) {
                throw new AppException("Can't get first pair stat");
            }
            return convertToStats(pairRs);

        } catch (SQLException ex) {
            throw new AppException("in connection" + ex);
        }
    }

    public Stats getNextPairStat() {
        try {
            if (!pairRs.next()) {
                return null;
            }
            return convertToStats(pairRs);

        } catch (SQLException ex) {
            throw new AppException("in connection" + ex);
        }
    }

    Stats convertToStats (ResultSet rs) throws SQLException {
        String first = rs.getString("first_term");
        String second = rs.getString("second_term");
        Integer count_indoc = rs.getInt("count_indoc");

        Integer counts_unordered_gap0 = rs.getInt("counts_unordered_gap0");
        Integer counts_unordered_gap1 = rs.getInt("counts_unordered_gap1");
        Integer counts_unordered_gap2 = rs.getInt("counts_unordered_gap2");
        Integer counts_unordered_gap3 = rs.getInt("counts_unordered_gap3");
        Integer counts_unordered_gap4 = rs.getInt("counts_unordered_gap4");
        Integer counts_unordered_gap5 = rs.getInt("counts_unordered_gap5");

        Integer counts_ordered_gap0 = rs.getInt("counts_ordered_gap0");
        Integer counts_ordered_gap1 = rs.getInt("counts_ordered_gap1");
        Integer counts_ordered_gap2 = rs.getInt("counts_ordered_gap2");
        Integer counts_ordered_gap3 = rs.getInt("counts_ordered_gap3");
        Integer counts_ordered_gap4 = rs.getInt("counts_ordered_gap4");
        Integer counts_ordered_gap5 = rs.getInt("counts_ordered_gap5");

        Integer counts_unordered_inwindow0 = rs.getInt("counts_unordered_inwindow0");
        Integer counts_unordered_inwindow1 = rs.getInt("counts_unordered_inwindow1");
        Integer counts_unordered_inwindow2 = rs.getInt("counts_unordered_inwindow2");
        Integer counts_unordered_inwindow3 = rs.getInt("counts_unordered_inwindow3");
        Integer counts_unordered_inwindow4 = rs.getInt("counts_unordered_inwindow4");
        Integer counts_unordered_inwindow5 = rs.getInt("counts_unordered_inwindow5");

        Integer[] counts_unordered_gap = new Integer[]{counts_unordered_gap0, counts_unordered_gap1,
                counts_unordered_gap2, counts_unordered_gap3, counts_unordered_gap4, counts_unordered_gap5};
        Integer[] counts_ordered_gap = new Integer[]{counts_ordered_gap0, counts_ordered_gap1,
                counts_ordered_gap2, counts_ordered_gap3, counts_ordered_gap4, counts_ordered_gap5};
        Integer[] counts_unordered_inwindow = new Integer[]{counts_unordered_inwindow0, counts_unordered_inwindow1,
                counts_unordered_inwindow2, counts_unordered_inwindow3, counts_unordered_inwindow4, counts_unordered_inwindow5};

        return new Stats(first, second, counts_ordered_gap, counts_unordered_gap, counts_unordered_inwindow, count_indoc);
    }

    public int getDocumentFrequency(String term) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from term_stats where term = '" + term + "'");
            if (!rs.next()) {
                throw new AppException("Term " + term + " not found in term stats");
            }
            return rs.getInt("document_frequency");

        } catch (SQLException ex) {
            throw new AppException("in connection" + ex);
        }
    }

    public int getCollectionFrequency(String term) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from term_stats where term = '" + term + "'");
            if (!rs.next()) {
                throw new AppException("Term " + term + " not found in term stats");
            }
            return rs.getInt("collection_frequency");

        } catch (SQLException ex) {
            throw new AppException("in connection" + ex);
        }
    }

    public void commit() {
        try {
            conn.commit();
        } catch (SQLException ex) {
            throw new AppException("in connection" + ex);
        }
    }

    public void addFeature(String first, String second, double indoc, double[] counts_ordered_gap, double[] counts_unordered_gap,
                           double[] counts_unordered_inwindow) {
        try {
            Statement stmt = conn.createStatement();
            String statement = "insert into features (first_term, second_term, count_indoc, ";
            for (int x = 0; x < 6; ++x) {
                statement += "counts_ordered_gap" + x + ", ";
            }
            for (int x = 0; x < 6; ++x) {
                statement += "counts_unordered_gap" + x + ", ";
            }
            for (int x = 0; x < 6; ++x) {
                statement += "counts_unordered_inwindow" + x;
                if (x < 5) {
                    statement += ", ";
                } else {
                    statement += ") ";
                }
            }
            statement += "values ('" + first + "', '" + second + "', " + indoc + ", ";
            for (int x = 0; x < 6; ++x) {
                statement += counts_ordered_gap[x] + ", ";
            }
            for (int x = 0; x < 6; ++x) {
                statement += counts_unordered_gap[x] + ", ";
            }
            for (int x = 0; x < 6; ++x) {
                statement += counts_unordered_inwindow[x];
                if (x < 5) {
                    statement += ", ";
                } else {
                    statement += ") ";
                }
            }
            stmt.execute(statement);
//            conn.commit();
        } catch (SQLException ex) {
            throw new AppException("in connection" + ex);
        }
    }

    public FeatureStats getFirstFeatureStat() {
        try {
            featureStmt = conn.createStatement();
            featureRs = featureStmt.executeQuery("select * from features order by first_term, second_term");
            if (!featureRs.next()) {
                throw new AppException("Can't get first feature stat");
            }
            return convertToFeatureStats(featureRs);

        } catch (SQLException ex) {
            throw new AppException("in connection" + ex);
        }
    }

    public FeatureStats getNextFeatureStat() {
        try {
            if (!featureRs.next()) {
                return null;
            }
            return convertToFeatureStats(featureRs);

        } catch (SQLException ex) {
            throw new AppException("in connection" + ex);
        }
    }

    FeatureStats convertToFeatureStats (ResultSet rs) throws SQLException {
        String first = rs.getString("first_term");
        String second = rs.getString("second_term");
        double count_indoc = rs.getInt("count_indoc");

        double counts_unordered_gap0 = rs.getInt("counts_unordered_gap0");
        double counts_unordered_gap1 = rs.getInt("counts_unordered_gap1");
        double counts_unordered_gap2 = rs.getInt("counts_unordered_gap2");
        double counts_unordered_gap3 = rs.getInt("counts_unordered_gap3");
        double counts_unordered_gap4 = rs.getInt("counts_unordered_gap4");
        double counts_unordered_gap5 = rs.getInt("counts_unordered_gap5");

        double counts_ordered_gap0 = rs.getInt("counts_ordered_gap0");
        double counts_ordered_gap1 = rs.getInt("counts_ordered_gap1");
        double counts_ordered_gap2 = rs.getInt("counts_ordered_gap2");
        double counts_ordered_gap3 = rs.getInt("counts_ordered_gap3");
        double counts_ordered_gap4 = rs.getInt("counts_ordered_gap4");
        double counts_ordered_gap5 = rs.getInt("counts_ordered_gap5");

        double counts_unordered_inwindow0 = rs.getInt("counts_unordered_inwindow0");
        double counts_unordered_inwindow1 = rs.getInt("counts_unordered_inwindow1");
        double counts_unordered_inwindow2 = rs.getInt("counts_unordered_inwindow2");
        double counts_unordered_inwindow3 = rs.getInt("counts_unordered_inwindow3");
        double counts_unordered_inwindow4 = rs.getInt("counts_unordered_inwindow4");
        double counts_unordered_inwindow5 = rs.getInt("counts_unordered_inwindow5");

        double[] counts_unordered_gap = new double[]{counts_unordered_gap0, counts_unordered_gap1,
                counts_unordered_gap2, counts_unordered_gap3, counts_unordered_gap4, counts_unordered_gap5};
        double[] counts_ordered_gap = new double[]{counts_ordered_gap0, counts_ordered_gap1,
                counts_ordered_gap2, counts_ordered_gap3, counts_ordered_gap4, counts_ordered_gap5};
        double[] counts_unordered_inwindow = new double[]{counts_unordered_inwindow0, counts_unordered_inwindow1,
                counts_unordered_inwindow2, counts_unordered_inwindow3, counts_unordered_inwindow4, counts_unordered_inwindow5};

        return new FeatureStats(first, second, counts_ordered_gap, counts_unordered_gap, counts_unordered_inwindow, count_indoc);
    }

}
