-- Create a table
CREATE TABLE IF NOT EXISTS my_table (
    id INT,
    name STRING,
    age INT
);

-- Insert data into the table
INSERT INTO TABLE my_table VALUES
    (1, 'John', 25),
    (2, 'Alice', 30),
    (3, 'Bob', 22);

-- Calculate the average age
SELECT AVG(age) AS average_age FROM my_table;