require "sqlite3"
require "csv"
require "pry"

# Create an open a new database
db = SQLite3::Database.new "traffic_law_content.db"

# Create a table
rows = db.execute <<-SQL
    create table keywords (
        keyword VARCHAR(200),
        cpc FLOAT,
        cpc_rank INT,
        volume_rank INT,
        difficulty_rank INT,
        aggregate_rank INT
    );
SQL

# Create the CSV rows and input their CPC rank. Set NULL value for future fields
CSV.foreach("trafficLaw_cpc_score.csv") do |row|
    db.execute("INSERT INTO keywords (keyword, cpc, cpc_rank, volume_rank, difficulty_rank, aggregate_rank)
                VALUES (?,?,?,?,?,?)", [row[0],row[1], row[2], 0, 0, 0])
end

# Read the Volume Rank CSV, query the Database and populate the volume rank field
CSV.foreach("trafficLaw_volume_score.csv") do |row|
    db.execute("UPDATE keywords SET volume_rank = ? WHERE keyword = ?", [row[2], row[0]])
end

# Read the Difficulty Rank CSV, query the Database and populate the difficulty rank field
CSV.foreach("trafficLaw_difficulty_score.csv") do |row|
    db.execute("UPDATE keywords SET difficulty_rank = ? WHERE keyword = ?", [row[2], row[0]])
end

# Iterate through every record in the database, sum their cpc, volume, and difficulty scores, and record their aggregate score in the database
db.execute("SELECT * FROM keywords") do |row|
    # binding.pry
    aggregate_rank = row[2] + row[3] + row[4]
    db.execute("UPDATE keywords SET aggregate_rank = ? WHERE keyword = ?", [aggregate_rank, row[0]])
end