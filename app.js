const questions = [
  
{
    code: "Spark-1",
    question: "What is Apache Spark primarily used for?",
    options: {
      a: "Image editing",
      b: "Distributed data processing",
      c: "File compression",
      d: "Cloud storage"
    },
    correct: "b"
  },
  {
    code: "Spark-2",
    question: "What makes Spark much faster than traditional systems?",
    options: {
      a: "Uses GPU acceleration",
      b: "In-memory processing",
      c: "Works only on small datasets",
      d: "Requires no cluster setup"
    },
    correct: "b"
  },
  {
    code: "Spark-3",
    question: "Which of the following is NOT a feature of Spark?",
    options: {
      a: "Fault tolerance",
      b: "Speed",
      c: "Manual memory management",
      d: "Distributed processing"
    },
    correct: "c"
  },
  {
    code: "Spark-4",
    question: "Spark supports which of the following languages?",
    options: {
      a: "Only Java",
      b: "Only Python",
      c: "Python, Java, Scala, and R",
      d: "Only Scala"
    },
    correct: "c"
  },
  {
    code: "Spark-5",
    question: "Which company uses Spark for real-time trip analysis?",
    options: {
      a: "Netflix",
      b: "Uber",
      c: "Amazon",
      d: "LinkedIn"
    },
    correct: "b"
  },

  {
    code: "Spark-6",
    question: "What is Spark SQL?",
    options: {
      a: "A web browser for Spark",
      b: "A module to query structured data using SQL",
      c: "A language for creating dashboards",
      d: "A Spark-based compiler"
    },
    correct: "b"
  },
  {
    code: "Spark-7",
    question: "Spark SQL queries are performed on which data structure?",
    options: {
      a: "SparkTables",
      b: "DataFrames",
      c: "RDDs only",
      d: "CSV files directly"
    },
    correct: "b"
  },
  {
    code: "Spark-8",
    question: "Which feature allows Spark SQL to handle large-scale data?",
    options: {
      a: "In-memory file transfer",
      b: "Distributed computation",
      c: "XML transformation",
      d: "CSV-only support"
    },
    correct: "b"
  },
  {
    code: "Spark-9",
    question: "Spark SQL can query data from which formats?",
    options: {
      a: "Only JSON",
      b: "Only CSV",
      c: "CSV, JSON, Parquet, Hive, and more",
      d: "Only Parquet"
    },
    correct: "c"
  },
  {
    code: "Spark-10",
    question: "Spark SQL is best described as:",
    options: {
      a: "A replacement for Python",
      b: "A high-level query interface for Spark",
      c: "A Spark debugging tool",
      d: "A visualization library"
    },
    correct: "b"
  },

  {
    code: "Spark-11",
    question: "A Spark DataFrame is most similar to which structure?",
    options: {
      a: "A Python dictionary",
      b: "A SQL table",
      c: "A JSON object",
      d: "A CSV file"
    },
    correct: "b"
  },
  {
    code: "Spark-12",
    question: "What do columns in a DataFrame represent?",
    options: {
      a: "Row numbers",
      b: "Data types",
      c: "Fields or attributes of the data",
      d: "Only numerical data"
    },
    correct: "c"
  },
  {
    code: "Spark-13",
    question: "What is one key advantage of using DataFrames?",
    options: {
      a: "Manual data partitioning",
      b: "Hides distributed computing complexity",
      c: "Requires complex setup",
      d: "Works only with CSVs"
    },
    correct: "b"
  },
  {
    code: "Spark-14",
    question: "DataFrames can be created from:",
    options: {
      a: "Only SQL queries",
      b: "Multiple sources like CSV, JSON, or databases",
      c: "Only manual typing",
      d: "Only text files"
    },
    correct: "b"
  },
  {
    code: "Spark-15",
    question: "Which operation can you perform on a DataFrame?",
    options: {
      a: "Compilation",
      b: "Transformation and aggregation",
      c: "Compression only",
      d: "None of these"
    },
    correct: "b"
  },

  {
    code: "Spark-16",
    question: "In Spark SQL, a table is conceptually equivalent to:",
    options: {
      a: "A JSON file",
      b: "A directory",
      c: "A DataFrame",
      d: "A Python list"
    },
    correct: "c"
  },
  {
    code: "Spark-17",
    question: "Which keyword retrieves specific columns from a table?",
    options: {
      a: "GET",
      b: "SELECT",
      c: "FETCH",
      d: "EXTRACT"
    },
    correct: "b"
  },
  {
    code: "Spark-18",
    question: "What does the asterisk (*) in a SELECT query mean?",
    options: {
      a: "Select all columns",
      b: "Multiply values",
      c: "Add columns",
      d: "None of these"
    },
    correct: "a"
  },
  {
    code: "Spark-19",
    question: "What defines a table’s structure in Spark SQL?",
    options: {
      a: "Rows only",
      b: "Columns and their data types",
      c: "File names",
      d: "Folder hierarchy"
    },
    correct: "b"
  },
  {
    code: "Spark-20",
    question: "Why is selecting specific columns useful?",
    options: {
      a: "Reduces memory usage and focuses on needed data",
      b: "Makes code longer",
      c: "Increases data redundancy",
      d: "None of these"
    },
    correct: "a"
  },

  {
    code: "Spark-21",
    question: "What does the WHERE clause do in Spark SQL?",
    options: {
      a: "Joins two tables",
      b: "Filters rows based on conditions",
      c: "Creates new tables",
      d: "Groups data"
    },
    correct: "b"
  },
  {
    code: "Spark-22",
    question: "Which operator checks multiple conditions together?",
    options: {
      a: "ADD",
      b: "AND / OR",
      c: "EQUALS",
      d: "INCREMENT"
    },
    correct: "b"
  },
  {
    code: "Spark-23",
    question: "What would WHERE Salary > 65000 do?",
    options: {
      a: "Selects employees with salary above 65,000",
      b: "Selects all employees",
      c: "Returns an error",
      d: "Deletes data"
    },
    correct: "a"
  },
  {
    code: "Spark-24",
    question: "Which operator ensures only one of multiple conditions is true?",
    options: {
      a: "AND",
      b: "OR",
      c: "NOT",
      d: "XOR"
    },
    correct: "b"
  },
  {
    code: "Spark-25",
    question: "Why is filtering data essential in analytics?",
    options: {
      a: "It hides errors",
      b: "It focuses analysis on relevant information",
      c: "It deletes unused columns",
      d: "It merges all data"
    },
    correct: "b"
  },

  {
    code: "Spark-26",
    question: "Which clause is used in Spark SQL to sort query results?",
    options: {
      a: "SORT",
      b: "ORDER BY",
      c: "GROUP BY",
      d: "SORTING"
    },
    correct: "b"
  },
  {
    code: "Spark-27",
    question: "What is the default sort order in Spark SQL?",
    options: {
      a: "Descending",
      b: "Random",
      c: "Ascending",
      d: "Lexical"
    },
    correct: "c"
  },
  {
    code: "Spark-28",
    question: "What does ORDER BY Salary DESC do?",
    options: {
      a: "Shows only high salaries",
      b: "Sorts salaries from high to low",
      c: "Filters salaries below average",
      d: "Removes duplicate salaries"
    },
    correct: "b"
  },
  {
    code: "Spark-29",
    question: "You can sort by multiple columns in Spark SQL.",
    options: {
      a: "True",
      b: "False"
    },
    correct: "a"
  },
  {
    code: "Spark-30",
    question: "Which query sorts departments alphabetically and salaries within each department from high to low?",
    options: {
      a: "ORDER BY Salary DESC, Department ASC",
      b: "ORDER BY Department ASC, Salary DESC",
      c: "ORDER BY Salary ASC, Department DESC",
      d: "ORDER BY Department DESC, Salary ASC"
    },
    correct: "b"
  },

  {
    code: "Spark-31",
    question: "Which function returns the number of rows in a table?",
    options: {
      a: "SUM()",
      b: "COUNT()",
      c: "AVG()",
      d: "MAX()"
    },
    correct: "b"
  },
  {
    code: "Spark-32",
    question: "Which function gives the average of numeric values?",
    options: {
      a: "MEAN()",
      b: "AVERAGE()",
      c: "AVG()",
      d: "MEDIAN()"
    },
    correct: "c"
  },
  {
    code: "Spark-33",
    question: "To find the highest salary, you use:",
    options: {
      a: "TOP()",
      b: "MAX()",
      c: "CEIL()",
      d: "LIMIT()"
    },
    correct: "b"
  },
  {
    code: "Spark-34",
    question: "What does SUM(Salary) return?",
    options: {
      a: "Average salary",
      b: "Total of all salaries",
      c: "Maximum salary",
      d: "Count of employees"
    },
    correct: "b"
  },
  {
    code: "Spark-35",
    question: "Aggregation functions are used to:",
    options: {
      a: "Filter rows",
      b: "Transform text",
      c: "Summarize data",
      d: "Sort columns"
    },
    correct: "c"
  },

  {
    code: "Spark-36",
    question: "What is the purpose of GROUP BY in Spark SQL?",
    options: {
      a: "Combine multiple tables",
      b: "Group rows by common values",
      c: "Filter rows based on condition",
      d: "Sort data alphabetically"
    },
    correct: "b"
  },
  {
    code: "Spark-37",
    question: "GROUP BY is often used with:",
    options: {
      a: "ORDER BY",
      b: "HAVING",
      c: "Aggregation functions",
      d: "DISTINCT"
    },
    correct: "c"
  },
  {
    code: "Spark-38",
    question: "SELECT Department, AVG(Salary) FROM Employees GROUP BY Department; does what?",
    options: {
      a: "Lists all employees",
      b: "Shows total salary for each department",
      c: "Calculates average salary per department",
      d: "Sorts employees by department"
    },
    correct: "c"
  },
  {
    code: "Spark-39",
    question: "GROUP BY can be used on multiple columns.",
    options: {
      a: "True",
      b: "False"
    },
    correct: "a"
  },
  {
    code: "Spark-40",
    question: "GROUP BY helps in:",
    options: {
      a: "Filtering data",
      b: "Aggregating by categories",
      c: "Joining tables",
      d: "Formatting output"
    },
    correct: "b"
  },

  {
    code: "Spark-41",
    question: "HAVING filters data:",
    options: {
      a: "Before grouping",
      b: "After grouping",
      c: "Before sorting",
      d: "During joining"
    },
    correct: "b"
  },
  {
    code: "Spark-42",
    question: "Which query finds departments with average salary > 60000?",
    options: {
      a: "WHERE AVG(Salary) > 60000",
      b: "HAVING AVG(Salary) > 60000",
      c: "FILTER AVG(Salary) > 60000",
      d: "SELECT AVG(Salary) > 60000"
    },
    correct: "b"
  },
  {
    code: "Spark-43",
    question: "HAVING can be used without GROUP BY.",
    options: {
      a: "True",
      b: "False"
    },
    correct: "b"
  },
  {
    code: "Spark-44",
    question: "Difference between WHERE and HAVING?",
    options: {
      a: "WHERE filters rows, HAVING filters groups",
      b: "Both filter before aggregation",
      c: "WHERE filters groups, HAVING filters rows",
      d: "No difference"
    },
    correct: "a"
  },
  {
    code: "Spark-45",
    question: "HAVING is typically used with which functions?",
    options: {
      a: "String functions",
      b: "Numeric functions",
      c: "Aggregation functions",
      d: "Date functions"
    },
    correct: "c"
  },

  {
    code: "Spark-46",
    question: "Which function rounds a numeric value to the nearest integer?",
    options: {
      a: "CEIL()",
      b: "FLOOR()",
      c: "ROUND()",
      d: "TRUNC()"
    },
    correct: "c"
  },
  {
    code: "Spark-47",
    question: "Which function converts text to lowercase?",
    options: {
      a: "TOLOWER()",
      b: "LOWER()",
      c: "SMALL()",
      d: "TEXTDOWN()"
    },
    correct: "b"
  },
  {
    code: "Spark-48",
    question: "The function CONCAT(Name, ' - ', Department) returns:",
    options: {
      a: "A numeric result",
      b: "Combined string with name and department",
      c: "A new table",
      d: "Filtered rows"
    },
    correct: "b"
  },
  {
    code: "Spark-49",
    question: "What does CEIL(Salary) do?",
    options: {
      a: "Rounds salary down",
      b: "Rounds salary up",
      c: "Converts to string",
      d: "Truncates decimals"
    },
    correct: "b"
  },
  {
    code: "Spark-50",
    question: "YEAR(date_column) extracts:",
    options: {
      a: "Full date",
      b: "Month",
      c: "Year part",
      d: "Time"
    },
    correct: "c"
  },

   {
    code: "Spark-51",
    question: "What does an INNER JOIN return?",
    options: {
      a: "Only unmatched rows",
      b: "All rows from both tables",
      c: "Only matching rows from both tables",
      d: "Rows with NULL values only"
    },
    correct: "c"
  },
  {
    code: "Spark-52",
    question: "In a join query, the ON clause specifies:",
    options: {
      a: "Columns to sort",
      b: "Columns to join on",
      c: "Columns to group",
      d: "Columns to filter"
    },
    correct: "b"
  },
  {
    code: "Spark-53",
    question: "What is the default join type in Spark SQL when using JOIN without specifying type?",
    options: {
      a: "LEFT JOIN",
      b: "RIGHT JOIN",
      c: "FULL JOIN",
      d: "INNER JOIN"
    },
    correct: "d"
  },
  {
    code: "Spark-54",
    question: "In the employee-department example, how is 'Mark' related to Bob and Charlie?",
    options: {
      a: "They are in the same department",
      b: "Mark manages both of them",
      c: "They share the same city",
      d: "They have no relation"
    },
    correct: "b"
  },
  {
    code: "Spark-55",
    question: "Joins help by:",
    options: {
      a: "Duplicating tables",
      b: "Combining related data from multiple tables",
      c: "Deleting records",
      d: "Sorting tables"
    },
    correct: "b"
  },

  {
    code: "Spark-56",
    question: "LEFT JOIN returns:",
    options: {
      a: "Only matching rows",
      b: "All rows from left table and matching rows from right",
      c: "All rows from right table",
      d: "Only unmatched rows"
    },
    correct: "b"
  },
  {
    code: "Spark-57",
    question: "If a record in the left table has no match in the right, what appears in the joined output?",
    options: {
      a: "0",
      b: "Blank row",
      c: "NULL values",
      d: "Error"
    },
    correct: "c"
  },
  {
    code: "Spark-58",
    question: "Which statement describes LEFT JOIN correctly?",
    options: {
      a: "All from right table",
      b: "All from both tables",
      c: "All from left table + matches from right",
      d: "Only rows that match"
    },
    correct: "c"
  },
  {
    code: "Spark-59",
    question: "LEFT JOIN can be thought of as:",
    options: {
      a: "Filtering",
      b: "Sorting",
      c: "Extending left table with right-side info",
      d: "Grouping"
    },
    correct: "c"
  },
  {
    code: "Spark-60",
    question: "In the analogy, a student without a teacher appears as:",
    options: {
      a: "Error",
      b: "NULL teacher",
      c: "Empty record",
      d: "Skipped entirely"
    },
    correct: "b"
  },

  {
    code: "Spark-61",
    question: "RIGHT JOIN returns:",
    options: {
      a: "All rows from left table",
      b: "All rows from right table with matches from left",
      c: "Only matched rows",
      d: "Rows with NULLs only"
    },
    correct: "b"
  },
  {
    code: "Spark-62",
    question: "FULL OUTER JOIN returns:",
    options: {
      a: "Rows from both tables with all matches",
      b: "Only rows with matches",
      c: "Only unmatched rows",
      d: "None of the above"
    },
    correct: "a"
  },
  {
    code: "Spark-63",
    question: "Which join ensures no data is lost from either table?",
    options: {
      a: "INNER JOIN",
      b: "LEFT JOIN",
      c: "RIGHT JOIN",
      d: "FULL OUTER JOIN"
    },
    correct: "d"
  },
  {
    code: "Spark-64",
    question: "If a department has no employees, what happens in RIGHT JOIN output?",
    options: {
      a: "It’s excluded",
      b: "Appears with NULL employee values",
      c: "Causes error",
      d: "Appears twice"
    },
    correct: "b"
  },
  {
    code: "Spark-65",
    question: "Match the join type: LEFT JOIN → keeps all from ___, RIGHT JOIN → keeps all from ___, FULL OUTER JOIN → keeps all from ___",
    options: {
      a: "Left, Right, Both",
      b: "Right, Left, Both",
      c: "Both, Right, Left",
      d: "Left, Both, Right"
    },
    correct: "a"
  },

  {
    code: "Spark-66",
    question: "In a query combining JOIN and GROUP BY, which happens first?",
    options: {
      a: "GROUP BY",
      b: "HAVING",
      c: "JOIN",
      d: "ORDER BY"
    },
    correct: "c"
  },
  {
    code: "Spark-67",
    question: "What does the HAVING clause filter?",
    options: {
      a: "Individual rows",
      b: "Grouped results",
      c: "Joined columns",
      d: "Joins themselves"
    },
    correct: "b"
  },
  {
    code: "Spark-68",
    question: "Which department was shown with total salary > 100000?",
    options: {
      a: "HR",
      b: "IT",
      c: "Finance",
      d: "Marketing"
    },
    correct: "b"
  },
  {
    code: "Spark-69",
    question: "Why combine JOIN with aggregation functions?",
    options: {
      a: "To delete duplicate rows",
      b: "To summarize data across tables",
      c: "To change data types",
      d: "To rename columns"
    },
    correct: "b"
  },
  {
    code: "Spark-70",
    question: "The role of GROUP BY in a JOIN query is to:",
    options: {
      a: "Sort results",
      b: "Group by selected keys after joining",
      c: "Filter before join",
      d: "Duplicate data"
    },
    correct: "b"
  },

  {
    code: "Spark-71",
    question: "The first step before joining tables is to:",
    options: {
      a: "Filter the data",
      b: "Identify the join key",
      c: "Aggregate values",
      d: "Create indexes"
    },
    correct: "b"
  },
  {
    code: "Spark-72",
    question: "Filtering early in Spark SQL helps because:",
    options: {
      a: "It increases data size",
      b: "It reduces computation load",
      c: "It adds more columns",
      d: "It slows performance"
    },
    correct: "b"
  },
  {
    code: "Spark-73",
    question: "Aliases in SQL are used to:",
    options: {
      a: "Rename tables or columns for readability",
      b: "Remove duplicate records",
      c: "Filter NULL values",
      d: "Sort the data"
    },
    correct: "a"
  },
  {
    code: "Spark-74",
    question: "Aggregations should be applied:",
    options: {
      a: "Before joining",
      b: "During filtering",
      c: "After joining",
      d: "Before selecting columns"
    },
    correct: "c"
  },
  {
    code: "Spark-75",
    question: "Spark SQL is best described as:",
    options: {
      a: "A traditional database",
      b: "A programming language",
      c: "A distributed SQL engine for big data",
      d: "A visualization tool"
    },
    correct: "c"
  },

  {
    code: "Spark-76",
    question: "In Spark SQL, a subquery inside the WHERE clause is mainly used for:",
    options: {
      a: "Creating new tables",
      b: "Filtering rows dynamically",
      c: "Changing schema structure",
      d: "Joining two datasets"
    },
    correct: "b"
  },
  {
    code: "Spark-77",
    question: "What does the inner query in a subquery do?",
    options: {
      a: "Drops existing tables",
      b: "Runs first and returns a temporary result",
      c: "Optimizes the Catalyst plan",
      d: "Executes last for summarization"
    },
    correct: "b"
  },
  {
    code: "Spark-78",
    question: "Which type of subquery may reduce performance due to row-wise execution?",
    options: {
      a: "Uncorrelated subquery",
      b: "Inline subquery",
      c: "Correlated subquery",
      d: "Temporary subquery"
    },
    correct: "c"
  },
  {
    code: "Spark-79",
    question: "What is a good alternative to correlated subqueries?",
    options: {
      a: "Materialized views",
      b: "Joins or window functions",
      c: "Nested CTEs",
      d: "Cross joins"
    },
    correct: "b"
  },
  {
    code: "Spark-80",
    question: "What is one advantage of using subqueries?",
    options: {
      a: "They permanently store results",
      b: "They reduce table size",
      c: "They make logic modular and readable",
      d: "They bypass the optimizer"
    },
    correct: "c"
  },

  {
    code: "Spark-81",
    question: "What is a view in Spark SQL?",
    options: {
      a: "A physical copy of data",
      b: "A stored query definition",
      c: "A cached result table",
      d: "A static dataset"
    },
    correct: "b"
  },
  {
    code: "Spark-82",
    question: "Which command creates a reusable view?",
    options: {
      a: "CREATE TABLE",
      b: "CREATE INDEX",
      c: "CREATE OR REPLACE VIEW",
      d: "CREATE FUNCTION"
    },
    correct: "c"
  },
  {
    code: "Spark-83",
    question: "What’s the difference between temporary and global views?",
    options: {
      a: "Temporary views persist after restart",
      b: "Global views share data across sessions",
      c: "Temporary views are faster",
      d: "Global views don’t require databases"
    },
    correct: "b"
  },
  {
    code: "Spark-84",
    question: "What happens when you query a view?",
    options: {
      a: "Spark fetches stored data",
      b: "Spark executes the underlying SQL dynamically",
      c: "It performs lazy evaluation",
      d: "It triggers caching by default"
    },
    correct: "b"
  },
  {
    code: "Spark-85",
    question: "Why are views useful in collaborative environments?",
    options: {
      a: "They allow schema modifications",
      b: "They hide raw data access",
      c: "They ensure consistent reusable logic",
      d: "They optimize joins automatically"
    },
    correct: "c"
  },

  {
    code: "Spark-86",
    question: "The CASE statement in Spark SQL is equivalent to:",
    options: {
      a: "switch-case in Java",
      b: "if-else in programming",
      c: "loop in Python",
      d: "a join condition"
    },
    correct: "b"
  },
  {
    code: "Spark-87",
    question: "Which query labels orders as High, Medium, or Low based on amount?",
    options: {
      a: "GROUP BY amount",
      b: "CASE WHEN condition",
      c: "LIMIT with ORDER BY",
      d: "HAVING amount > 500"
    },
    correct: "b"
  },
  {
    code: "Spark-88",
    question: "What function can replace simple CASE use for defaults?",
    options: {
      a: "ROUND()",
      b: "COALESCE()",
      c: "RAND()",
      d: "LENGTH()"
    },
    correct: "b"
  },
  {
    code: "Spark-89",
    question: "Where are CASE statements especially useful?",
    options: {
      a: "Only during joins",
      b: "While performing aggregations and KPIs",
      c: "For schema definition",
      d: "Inside CREATE VIEW"
    },
    correct: "b"
  },
  {
    code: "Spark-90",
    question: "What is the main caution when using deeply nested CASE statements?",
    options: {
      a: "They break queries",
      b: "They slow Spark startup",
      c: "They hurt readability",
      d: "They block parallelism"
    },
    correct: "c"
  },

  {
    code: "Spark-91",
    question: "Which Spark SQL function joins two string columns with a space?",
    options: {
      a: "CONCAT()",
      b: "MERGE()",
      c: "COMBINE()",
      d: "JOIN()"
    },
    correct: "a"
  },
  {
    code: "Spark-92",
    question: "What function converts all text to uppercase?",
    options: {
      a: "INITCAP()",
      b: "UPPER()",
      c: "CASECHANGE()",
      d: "CAPITALIZE()"
    },
    correct: "b"
  },
  {
    code: "Spark-93",
    question: "How do you remove extra spaces from text fields?",
    options: {
      a: "CUT()",
      b: "REMOVE()",
      c: "TRIM()",
      d: "CLEAN()"
    },
    correct: "c"
  },
  {
    code: "Spark-94",
    question: "Which function helps extract parts of a string using regex?",
    options: {
      a: "FIND()",
      b: "REGEXP_EXTRACT()",
      c: "REGEX_SPLIT()",
      d: "TEXT_MATCH()"
    },
    correct: "b"
  },
  {
    code: "Spark-95",
    question: "Why are string functions critical in analytics?",
    options: {
      a: "They help parse and clean text data efficiently",
      b: "They store metadata",
      c: "They replace joins",
      d: "They improve indexing"
    },
    correct: "a"
  },

  {
    code: "Spark-96",
    question: "Which function returns the current date in Spark SQL?",
    options: {
      a: "NOW()",
      b: "TODAY()",
      c: "CURRENT_DATE()",
      d: "SYS_DATE()"
    },
    correct: "c"
  },
  {
    code: "Spark-97",
    question: "To find records older than 30 days, which function helps compute duration?",
    options: {
      a: "DATE_ADD()",
      b: "DATEDIFF()",
      c: "DAY()",
      d: "TIMESTAMPDIFF()"
    },
    correct: "b"
  },
  {
    code: "Spark-98",
    question: "Which function extracts month from a date column?",
    options: {
      a: "PARTITION()",
      b: "MONTH()",
      c: "EXTRACT_MONTH()",
      d: "DATEPART()"
    },
    correct: "b"
  },
  {
    code: "Spark-99",
    question: "Why are timezone functions like FROM_UTC_TIMESTAMP() useful?",
    options: {
      a: "They remove duplicate timestamps",
      b: "They standardize data across regions",
      c: "They drop nulls automatically",
      d: "They partition data by timezone"
    },
    correct: "b"
  },
  {
    code: "Spark-100",
    question: "Why are date functions key to time-series analysis?",
    options: {
      a: "They speed up joins",
      b: "They add synthetic columns",
      c: "They help extract trends and cycles",
      d: "They improve caching"
    },
    correct: "c"
  },
  {
    code: "Spark-101",
    question: "Which Spark SQL function returns the absolute value of a number?",
    options: {
      a: "CEIL()",
      b: "ABS()",
      c: "SIGN()",
      d: "ROUND()"
    },
    correct: "b"
  },
  {
    code: "Spark-102",
    question: "What does the ROUND() function do?",
    options: {
      a: "Rounds a number to the nearest integer or specified decimal places",
      b: "Returns only integer division results",
      c: "Converts numbers to strings",
      d: "Randomly changes digits"
    },
    correct: "a"
  },
  {
    code: "Spark-103",
    question: "Which function can generate random decimal numbers between 0 and 1?",
    options: {
      a: "RAND()",
      b: "RANDOM()",
      c: "SAMPLE()",
      d: "SEED()"
    },
    correct: "a"
  },
  {
    code: "Spark-104",
    question: "What is the output of POW(2, 3)?",
    options: {
      a: "5",
      b: "6",
      c: "8",
      d: "9"
    },
    correct: "c"
  },
  {
    code: "Spark-105",
    question: "Why are math functions important in data pipelines?",
    options: {
      a: "They help modify schemas",
      b: "They improve SQL readability",
      c: "They support normalization, scaling, and metric calculations",
      d: "They remove duplicates"
    },
    correct: "c"
  },

  {
    code: "Spark-106",
    question: "What do window functions in Spark SQL enable?",
    options: {
      a: "Filtering tables",
      b: "Performing calculations across rows related to the current row",
      c: "Changing schema structure",
      d: "Creating new databases"
    },
    correct: "b"
  },
  {
    code: "Spark-107",
    question: "Which keyword defines partitions in a window function?",
    options: {
      a: "ORDER BY",
      b: "PARTITION BY",
      c: "GROUP BY",
      d: "RANGE BY"
    },
    correct: "b"
  },
  {
    code: "Spark-108",
    question: "What does ROW_NUMBER() do?",
    options: {
      a: "Counts nulls",
      b: "Returns sequential numbers for rows in each partition",
      c: "Aggregates rows by group",
      d: "Generates a hash key"
    },
    correct: "b"
  },
  {
    code: "Spark-109",
    question: "What is the purpose of ORDER BY inside OVER()?",
    options: {
      a: "Defines computation sequence within each partition",
      b: "Sorts entire dataset permanently",
      c: "Removes duplicates",
      d: "Filters rows by order"
    },
    correct: "a"
  },
  {
    code: "Spark-110",
    question: "Which function calculates a running sum of sales?",
    options: {
      a: "SUM(sales)",
      b: "SUM(sales) OVER(ORDER BY date)",
      c: "TOTAL(sales)",
      d: "RANGE_SUM(sales)"
    },
    correct: "b"
  },

  {
    code: "Spark-111",
    question: "What does a CTE in Spark SQL provide?",
    options: {
      a: "Temporary query results for reuse",
      b: "Permanent view storage",
      c: "Faster joins",
      d: "Schema migration"
    },
    correct: "a"
  },
  {
    code: "Spark-112",
    question: "What keyword is used to start a CTE?",
    options: {
      a: "WITH",
      b: "TEMPORARY",
      c: "CREATE",
      d: "SELECT"
    },
    correct: "a"
  },
  {
    code: "Spark-113",
    question: "Why are CTEs better than subqueries in some cases?",
    options: {
      a: "They run slower",
      b: "They enhance readability and reusability",
      c: "They store data permanently",
      d: "They change schema"
    },
    correct: "b"
  },
  {
    code: "Spark-114",
    question: "Can you reference the same CTE multiple times in one query?",
    options: {
      a: "No",
      b: "Yes",
      c: "Only if it’s cached",
      d: "Only in joins"
    },
    correct: "b"
  },
  {
    code: "Spark-115",
    question: "What happens to a CTE after the main query runs?",
    options: {
      a: "It persists",
      b: "It is dropped automatically",
      c: "It is saved to disk",
      d: "It becomes a global view"
    },
    correct: "b"
  },

  {
    code: "Spark-116",
    question: "Which clause is essential for grouping in Spark SQL?",
    options: {
      a: "HAVING",
      b: "GROUP BY",
      c: "ORDER BY",
      d: "JOIN"
    },
    correct: "b"
  },
  {
    code: "Spark-117",
    question: "What function returns the number of rows in each group?",
    options: {
      a: "COUNT()",
      b: "SUM()",
      c: "MAX()",
      d: "LEN()"
    },
    correct: "a"
  },
  {
    code: "Spark-118",
    question: "How can multiple aggregates be computed together?",
    options: {
      a: "Using multiple GROUP BYs",
      b: "Using the GROUPING SETS clause",
      c: "Running separate queries",
      d: "Using CASE statements"
    },
    correct: "b"
  },
  {
    code: "Spark-119",
    question: "What’s the purpose of HAVING?",
    options: {
      a: "Filters columns",
      b: "Filters aggregated results",
      c: "Creates indexes",
      d: "Sorts partitions"
    },
    correct: "b"
  },
  {
    code: "Spark-120",
    question: "Why are aggregations crucial in analytics?",
    options: {
      a: "They replace joins",
      b: "They summarize large datasets into insights",
      c: "They clean text data",
      d: "They store schemas"
    },
    correct: "b"
  },

  {
    code: "Spark-121",
    question: "What does the get_json_object() function do?",
    options: {
      a: "Converts JSON to CSV",
      b: "Extracts specific fields from a JSON string",
      c: "Removes invalid JSON entries",
      d: "Merges two JSON objects"
    },
    correct: "b"
  },
  {
    code: "Spark-122",
    question: "Which function converts a column or struct to a JSON string?",
    options: {
      a: "TO_JSON()",
      b: "TO_JSON_STRING()",
      c: "TO_JSON_OBJECT()",
      d: "STRINGIFY()"
    },
    correct: "a"
  },
  {
    code: "Spark-123",
    question: "How does from_json() differ from get_json_object()?",
    options: {
      a: "It reads binary data",
      b: "It parses JSON into a struct with defined schema",
      c: "It merges arrays",
      d: "It formats date fields"
    },
    correct: "b"
  },
  {
    code: "Spark-124",
    question: "Why is schema definition important when using from_json()?",
    options: {
      a: "It improves read speed",
      b: "It avoids runtime parsing errors",
      c: "It compresses data",
      d: "It adds metadata"
    },
    correct: "b"
  },
  {
    code: "Spark-125",
    question: "Why are JSON functions essential for modern data systems?",
    options: {
      a: "They store numbers efficiently",
      b: "They help handle nested semi-structured data",
      c: "They improve join speed",
      d: "They manage indexes"
    },
    correct: "b"
  },

  {
    code: "Spark-126",
    question: "Why do we use UDFs in Spark SQL?",
    options: {
      a: "To store data permanently",
      b: "To define custom logic beyond built-in functions",
      c: "To connect Spark to external databases",
      d: "To optimize physical query plans"
    },
    correct: "b"
  },
  {
    code: "Spark-127",
    question: "Which command registers a UDF in Spark SQL?",
    options: {
      a: "CREATE TABLE",
      b: "CREATE FUNCTION",
      c: "CREATE INDEX",
      d: "CREATE ROLE"
    },
    correct: "b"
  },
  {
    code: "Spark-128",
    question: "Why can UDFs reduce performance?",
    options: {
      a: "They bypass the Catalyst optimizer",
      b: "They serialize data between JVM and Python",
      c: "They use more network bandwidth",
      d: "They disable caching"
    },
    correct: "b"
  },
  {
    code: "Spark-129",
    question: "What is a Pandas UDF?",
    options: {
      a: "A debugging tool",
      b: "A vectorized UDF using Pandas for performance",
      c: "A Spark SQL optimizer function",
      d: "A Python data storage format"
    },
    correct: "b"
  },
  {
    code: "Spark-130",
    question: "When should you avoid UDFs?",
    options: {
      a: "When built-in or higher-order functions can do the job",
      b: "When the data is small",
      c: "When using broadcast joins",
      d: "When writing JSON files"
    },
    correct: "a"
  },

  {
    code: "Spark-131",
    question: "What does a PIVOT operation do?",
    options: {
      a: "Converts columns into rows",
      b: "Converts rows into columns",
      c: "Deletes duplicate rows",
      d: "Groups by all columns"
    },
    correct: "b"
  },
  {
    code: "Spark-132",
    question: "What is the reverse of a pivot?",
    options: {
      a: "GROUP BY",
      b: "UNPIVOT",
      c: "JOIN",
      d: "ROLLUP"
    },
    correct: "b"
  },
  {
    code: "Spark-133",
    question: "Which clause is required when performing a pivot?",
    options: {
      a: "ORDER BY",
      b: "GROUP BY with an aggregate function",
      c: "WHERE",
      d: "LIMIT"
    },
    correct: "b"
  },
  {
    code: "Spark-134",
    question: "Which operation would you use to summarize quarterly data across multiple regions?",
    options: {
      a: "UNPIVOT",
      b: "PIVOT",
      c: "MERGE",
      d: "UNION"
    },
    correct: "b"
  },
  {
    code: "Spark-135",
    question: "Why can pivoting be memory-intensive?",
    options: {
      a: "It duplicates all columns",
      b: "It expands intermediate data and requires grouping",
      c: "It compresses partitions",
      d: "It deletes cache memory"
    },
    correct: "b"
  },

  {
    code: "Spark-136",
    question: "What does caching achieve in Spark SQL?",
    options: {
      a: "Stores data temporarily in memory for faster access",
      b: "Deletes redundant rows",
      c: "Encrypts sensitive data",
      d: "Moves data to disk storage"
    },
    correct: "a"
  },
  {
    code: "Spark-137",
    question: "When should you use broadcast joins?",
    options: {
      a: "When both tables are large",
      b: "When one table is small enough to fit in memory",
      c: "When both tables are partitioned",
      d: "When using window functions"
    },
    correct: "b"
  },
  {
    code: "Spark-138",
    question: "What does the EXPLAIN command show?",
    options: {
      a: "Table schema",
      b: "Query execution plan",
      c: "Metadata",
      d: "Lineage"
    },
    correct: "b"
  },
  {
    code: "Spark-139",
    question: "What is Spark’s query optimizer called?",
    options: {
      a: "Catalyst",
      b: "Newton",
      c: "Falcon",
      d: "Orca"
    },
    correct: "a"
  },
  {
    code: "Spark-140",
    question: "Which format improves read performance compared to CSV?",
    options: {
      a: "JSON",
      b: "Parquet",
      c: "TXT",
      d: "XML"
    },
    correct: "b"
  },

  {
    code: "Spark-141",
    question: "What is partitioning in Spark SQL?",
    options: {
      a: "Breaking data into folders by key columns",
      b: "Compressing data into one file",
      c: "Encrypting data at rest",
      d: "Sorting by timestamp"
    },
    correct: "a"
  },
  {
    code: "Spark-142",
    question: "What is bucketing mainly used for?",
    options: {
      a: "Reducing duplicates",
      b: "Optimizing joins and aggregations",
      c: "Filtering data faster",
      d: "Caching tables"
    },
    correct: "b"
  },
  {
    code: "Spark-143",
    question: "What happens if you create too many partitions?",
    options: {
      a: "Spark crashes",
      b: "Overhead increases and performance drops",
      c: "Data gets duplicated",
      d: "Queries become read-only"
    },
    correct: "b"
  },
  {
    code: "Spark-144",
    question: "Which statement creates a bucketed table?",
    options: {
      a: "CLUSTERED BY (...) INTO ... BUCKETS",
      b: "PARTITIONED BY (...)",
      c: "CACHE TABLE ...",
      d: "EXPLAIN TABLE ..."
    },
    correct: "a"
  },
  {
    code: "Spark-145",
    question: "Why does partitioning improve performance?",
    options: {
      a: "It loads only relevant partitions during queries",
      b: "It merges files automatically",
      c: "It converts rows into columns",
      d: "It enables replication"
    },
    correct: "a"
  },

  {
    code: "Spark-146",
    question: "Which type of join returns only matching rows from both tables?",
    options: {
      a: "LEFT JOIN",
      b: "RIGHT JOIN",
      c: "INNER JOIN",
      d: "FULL JOIN"
    },
    correct: "c"
  },
  {
    code: "Spark-147",
    question: "Which operation combines datasets vertically?",
    options: {
      a: "JOIN",
      b: "UNION",
      c: "CROSS JOIN",
      d: "PIVOT"
    },
    correct: "b"
  },
  {
    code: "Spark-148",
    question: "What does INTERSECT return?",
    options: {
      a: "Rows in both datasets",
      b: "Rows unique to first dataset",
      c: "All rows combined",
      d: "Rows not matching"
    },
    correct: "a"
  },
  {
    code: "Spark-149",
    question: "What join strategy is best for small and large tables?",
    options: {
      a: "Shuffle join",
      b: "Broadcast join",
      c: "Nested loop",
      d: "Sort-merge"
    },
    correct: "b"
  },
  {
    code: "Spark-150",
    question: "Which set operation removes duplicates automatically?",
    options: {
      a: "UNION",
      b: "UNION ALL",
      c: "EXCEPT",
      d: "INTERSECT ALL"
    },
    correct: "a"
  },
  {
    code: "Spark-151",
    question: "What does a window function do?",
    options: {
      a: "Deletes duplicate rows",
      b: "Performs calculations across a range of rows",
      c: "Changes column names",
      d: "Converts rows to columns"
    },
    correct: "b"
  },
  {
    code: "Spark-152",
    question: "Which clause defines the window range?",
    options: {
      a: "USING",
      b: "OVER",
      c: "GROUP BY",
      d: "HAVING"
    },
    correct: "b"
  },
  {
    code: "Spark-153",
    question: "What does LAG() return?",
    options: {
      a: "Future value",
      b: "Previous row’s value",
      c: "Running total",
      d: "Count of rows"
    },
    correct: "b"
  },
  {
    code: "Spark-154",
    question: "Which function helps in calculating moving averages?",
    options: {
      a: "SUM() OVER",
      b: "RANK()",
      c: "COUNT()",
      d: "GROUP BY"
    },
    correct: "a"
  },
  {
    code: "Spark-155",
    question: "What should be done for large partitions?",
    options: {
      a: "Disable caching",
      b: "Use smaller window frames",
      c: "Disable joins",
      d: "Drop columns"
    },
    correct: "b"
  },

  {
    code: "Spark-156",
    question: "What does ROLLUP generate?",
    options: {
      a: "Only overall totals",
      b: "Hierarchical subtotals and grand totals",
      c: "Randomized aggregates",
      d: "Distinct counts only"
    },
    correct: "b"
  },
  {
    code: "Spark-157",
    question: "What is the function of CUBE?",
    options: {
      a: "Compresses data",
      b: "Produces all possible combinations of grouping columns",
      c: "Removes duplicates",
      d: "Sorts columns alphabetically"
    },
    correct: "b"
  },
  {
    code: "Spark-158",
    question: "Which keyword identifies subtotal rows?",
    options: {
      a: "GROUPING_ID()",
      b: "WINDOW_ID()",
      c: "SUM_ID()",
      d: "ORDER_ID()"
    },
    correct: "a"
  },
  {
    code: "Spark-159",
    question: "Why are rollups efficient?",
    options: {
      a: "They compute all summaries in one pass",
      b: "They use disk caching",
      c: "They convert to JSON",
      d: "They skip partitions"
    },
    correct: "a"
  },
  {
    code: "Spark-160",
    question: "What is a common use case for GROUPING SETS?",
    options: {
      a: "OLAP-style reporting",
      b: "Encryption",
      c: "Streaming",
      d: "Serialization"
    },
    correct: "a"
  },

  {
    code: "Spark-161",
    question: "What type of data does Structured Streaming handle?",
    options: {
      a: "Static",
      b: "Real-time",
      c: "Archived",
      d: "Sampled"
    },
    correct: "b"
  },
  {
    code: "Spark-162",
    question: "What is a micro-batch?",
    options: {
      a: "A backup process",
      b: "A small time window of streaming data",
      c: "A partition",
      d: "A join type"
    },
    correct: "b"
  },
  {
    code: "Spark-163",
    question: "What ensures fault tolerance in streaming?",
    options: {
      a: "Broadcast joins",
      b: "Checkpointing",
      c: "Shuffling",
      d: "Sorting"
    },
    correct: "b"
  },
  {
    code: "Spark-164",
    question: "What does a watermark do?",
    options: {
      a: "Adds security marks",
      b: "Defines lateness threshold for streaming data",
      c: "Tracks schema",
      d: "Compresses output"
    },
    correct: "b"
  },
  {
    code: "Spark-165",
    question: "Which sink stores streaming output persistently?",
    options: {
      a: "Parquet",
      b: "Console",
      c: "Memory",
      d: "Log"
    },
    correct: "a"
  },

  {
    code: "Spark-166",
    question: "What is a catalog in Spark SQL?",
    options: {
      a: "A data encryption key",
      b: "A system that stores metadata about databases and tables",
      c: "A backup tool",
      d: "A scheduler"
    },
    correct: "b"
  },
  {
    code: "Spark-167",
    question: "Which command lists all databases?",
    options: {
      a: "SHOW DATABASES",
      b: "DESCRIBE DATABASE",
      c: "LIST ALL",
      d: "SHOW TABLES"
    },
    correct: "a"
  },
  {
    code: "Spark-168",
    question: "Why is metadata important?",
    options: {
      a: "It controls encryption",
      b: "It ensures consistency across datasets",
      c: "It increases speed",
      d: "It deletes duplicates"
    },
    correct: "b"
  },
  {
    code: "Spark-169",
    question: "What does ALTER TABLE do?",
    options: {
      a: "Modifies table structure safely",
      b: "Deletes data",
      c: "Compresses partitions",
      d: "Creates new schema"
    },
    correct: "a"
  },
  {
    code: "Spark-170",
    question: "Which metastore is commonly used with Spark?",
    options: {
      a: "Hive Metastore",
      b: "JSON Metastore",
      c: "Redis Store",
      d: "Kafka"
    },
    correct: "a"
  },

  {
    code: "Spark-171",
    question: "What is RBAC in Spark SQL?",
    options: {
      a: "Role-Based Access Control",
      b: "Random Binary Access Controller",
      c: "Record-Based Aggregate Command",
      d: "Row Bucket Allocation Component"
    },
    correct: "a"
  },
  {
    code: "Spark-172",
    question: "Which command grants permission to a user role?",
    options: {
      a: "GRANT SELECT",
      b: "INSERT TABLE",
      c: "SHOW PRIVILEGES",
      d: "CREATE ROLE"
    },
    correct: "a"
  },
  {
    code: "Spark-173",
    question: "What is data lineage?",
    options: {
      a: "The origin and transformation history of data",
      b: "The row order in a table",
      c: "The compression type",
      d: "The cache size"
    },
    correct: "a"
  },
  {
    code: "Spark-174",
    question: "How is data protected during transit?",
    options: {
      a: "By using TLS encryption",
      b: "By caching",
      c: "By partitioning",
      d: "By replication"
    },
    correct: "a"
  },
  {
    code: "Spark-175",
    question: "Why is governance important?",
    options: {
      a: "Ensures compliance, accountability, and consistency",
      b: "Increases file size",
      c: "Replaces databases",
      d: "Speeds up queries only"
    },
    correct: "a"
  },

  {
    code: "Spark-176",
    question: "Which component allows Spark SQL to directly recognize existing Hive tables?",
    options: {
      a: "Hive CLI",
      b: "Hive Metastore",
      c: "HiveServer2",
      d: "Hive Executor"
    },
    correct: "b"
  },
  {
    code: "Spark-177",
    question: "Which Spark SQL connector is used to interact with relational databases?",
    options: {
      a: "ODBC",
      b: "JDBC",
      c: "REST",
      d: "RPC"
    },
    correct: "b"
  },
  {
    code: "Spark-178",
    question: "What is stored in the metastore for an external table?",
    options: {
      a: "Data and metadata",
      b: "Only data",
      c: "Only metadata",
      d: "Neither"
    },
    correct: "c"
  },
  {
    code: "Spark-179",
    question: "What is the main advantage of Spark SQL’s integration with external systems?",
    options: {
      a: "Reduces data redundancy",
      b: "Increases latency",
      c: "Requires constant data copying",
      d: "Disables interoperability"
    },
    correct: "a"
  },
  {
    code: "Spark-180",
    question: "Which statement defines a Parquet-based external table?",
    options: {
      a: "CREATE TABLE logs USING parquet OPTIONS(path '/data/logs/2025/')",
      b: "CREATE TABLE ext USING csv OPTIONS(path '/data')",
      c: "CREATE EXTERNAL TABLE logs AS SELECT * FROM parquet",
      d: "CREATE DATABASE logs USING parquet"
    },
    correct: "a"
  },

  {
    code: "Spark-181",
    question: "What does Spark’s ML SQL extension allow users to do?",
    options: {
      a: "Write ML models only in Python",
      b: "Train and predict directly using SQL",
      c: "Export ML models to CSV",
      d: "Execute ML on local mode only"
    },
    correct: "b"
  },
  {
    code: "Spark-182",
    question: "What SQL statement creates a temporary view for training features?",
    options: {
      a: "CREATE OR REPLACE MODEL",
      b: "CREATE OR REPLACE TEMP VIEW",
      c: "CREATE TEMP MODEL",
      d: "CREATE TRAIN VIEW"
    },
    correct: "b"
  },
  {
    code: "Spark-183",
    question: "Which model type is used for predicting sales totals?",
    options: {
      a: "DecisionTreeClassifier",
      b: "KMeans",
      c: "LinearRegression",
      d: "RandomForest"
    },
    correct: "c"
  },
  {
    code: "Spark-184",
    question: "What SQL function is used to make predictions from a trained model?",
    options: {
      a: "PREDICT()",
      b: "APPLY()",
      c: "INFER()",
      d: "FORECAST()"
    },
    correct: "a"
  },
  {
    code: "Spark-185",
    question: "Why is ML integration beneficial for non-programmers?",
    options: {
      a: "It removes SQL support",
      b: "It hides all ML models",
      c: "It enables ML workflows without coding",
      d: "It limits models to regression only"
    },
    correct: "c"
  },

  {
    code: "Spark-186",
    question: "What library adds graph processing capability to Spark SQL?",
    options: {
      a: "SparkGraph",
      b: "GraphFrames",
      c: "Neo4jConnector",
      d: "GraphXSQL"
    },
    correct: "b"
  },
  {
    code: "Spark-187",
    question: "How are entities and relationships represented in graph analytics?",
    options: {
      a: "Edges and Vertices",
      b: "Points and Lines",
      c: "Tables and Columns",
      d: "Keys and Values"
    },
    correct: "a"
  },
  {
    code: "Spark-188",
    question: "Which library adds geospatial processing to Spark SQL?",
    options: {
      a: "GeoSpark",
      b: "Sedona",
      c: "GeoPandas",
      d: "MapReduce"
    },
    correct: "b"
  },
  {
    code: "Spark-189",
    question: "Which function checks if a store is within a city boundary?",
    options: {
      a: "ST_Distance()",
      b: "ST_Within()",
      c: "ST_Intersect()",
      d: "ST_Touches()"
    },
    correct: "b"
  },
  {
    code: "Spark-190",
    question: "What type of analytics benefits most from graph and spatial queries?",
    options: {
      a: "Text summarization",
      b: "Social network and logistics analytics",
      c: "File compression",
      d: "Binary data streaming"
    },
    correct: "b"
  },

  {
    code: "Spark-191",
    question: "What does ACID stand for in Delta Lake?",
    options: {
      a: "Atomicity, Consistency, Isolation, Durability",
      b: "Accuracy, Control, Integrity, Distribution",
      c: "Automatic, Central, Indexed, Dataflow",
      d: "Analytic, Concurrent, Instant, Durable"
    },
    correct: "a"
  },
  {
    code: "Spark-192",
    question: "What file tracks all Delta table transactions?",
    options: {
      a: "_spark_log",
      b: "_delta_log",
      c: "delta_history",
      d: "metadata.json"
    },
    correct: "b"
  },
  {
    code: "Spark-193",
    question: "Which command enables querying older table versions?",
    options: {
      a: "SELECT * FROM sales HISTORY",
      b: "SELECT * FROM sales VERSION AS OF 3",
      c: "SELECT VERSION(3) FROM sales",
      d: "SELECT * FROM sales BACKUP 3"
    },
    correct: "b"
  },
  {
    code: "Spark-194",
    question: "How does Delta Lake handle schema evolution?",
    options: {
      a: "Blocks all changes",
      b: "Deletes old data",
      c: "Supports adding columns safely",
      d: "Restarts Spark sessions"
    },
    correct: "c"
  },
  {
    code: "Spark-195",
    question: "What architectural model is built on Delta Lake principles?",
    options: {
      a: "Data Mart",
      b: "Lakehouse",
      c: "OLTP System",
      d: "Star Schema"
    },
    correct: "b"
  },

  {
    code: "Spark-196",
    question: "Which statement correctly defines a Parquet table stored on S3?",
    options: {
      a: "CREATE TABLE cloud_sales USING parquet OPTIONS(path 's3://company/sales_data/')",
      b: "CREATE TABLE cloud_sales FROM S3 USING csv",
      c: "CREATE DATABASE cloud_sales LOCATION 's3://company'",
      d: "CREATE VIEW cloud_sales ON s3_data"
    },
    correct: "a"
  },
  {
    code: "Spark-197",
    question: "What Spark feature enables dynamic runtime query optimization?",
    options: {
      a: "Static Execution",
      b: "Adaptive Query Execution (AQE)",
      c: "Delta Schema Evolution",
      d: "Hive Caching"
    },
    correct: "b"
  },
  {
    code: "Spark-198",
    question: "Which configuration enables adaptive execution?",
    options: {
      a: "SET spark.sql.execution.enabled = true",
      b: "SET spark.sql.adaptive.enabled = true",
      c: "SET adaptive.execution = on",
      d: "SET spark.dynamic.mode = adaptive"
    },
    correct: "b"
  },
  {
    code: "Spark-199",
    question: "What benefit does running Spark SQL on cloud platforms provide?",
    options: {
      a: "Fixed compute capacity",
      b: "Manual scaling",
      c: "Elasticity and high availability",
      d: "On-premise dependency"
    },
    correct: "c"
  },
  {
    code: "Spark-200",
    question: "Which tools help monitor and manage cloud-based Spark SQL performance?",
    options: {
      a: "GitHub and Jenkins",
      b: "IAM and Terraform",
      c: "Auto-termination and cost tracking",
      d: "Hadoop and Sqoop"
    },
    correct: "c"
  },
  {
    code: "Spark-201",
    question: "What does the Spark Web UI primarily help users analyze?",
    options: {
      a: "Cluster deployment scripts",
      b: "Query execution stages and DAGs",
      c: "Storage metadata",
      d: "File format schemas"
    },
    correct: "b"
  },
  {
    code: "Spark-202",
    question: "Which command reveals Spark’s logical and physical query plans?",
    options: {
      a: "EXPLAIN EXTENDED",
      b: "ANALYZE PLAN",
      c: "SHOW QUERY",
      d: "TRACE EXECUTION"
    },
    correct: "a"
  },
  {
    code: "Spark-203",
    question: "What logging system does Spark use by default?",
    options: {
      a: "Prometheus",
      b: "Log4j",
      c: "Grafana",
      d: "Kibana"
    },
    correct: "b"
  },
  {
    code: "Spark-204",
    question: "Which setting enables Spark event logging for history tracking?",
    options: {
      a: "spark.history.enable",
      b: "spark.eventLog.enabled",
      c: "spark.logs.track",
      d: "spark.debug.sql"
    },
    correct: "b"
  },
  {
    code: "Spark-205",
    question: "What metric issue often signals data skew?",
    options: {
      a: "Uniform task durations",
      b: "Excess shuffle time in one stage",
      c: "Fewer partitions than cores",
      d: "Low memory utilization"
    },
    correct: "b"
  },

  {
    code: "Spark-206",
    question: "What is the main role of the Catalyst optimizer?",
    options: {
      a: "Convert SQL into execution plans",
      b: "Manage Spark UI logs",
      c: "Allocate cluster memory",
      d: "Serialize RDDs"
    },
    correct: "a"
  },
  {
    code: "Spark-207",
    question: "Which optimization technique reorders filters and joins automatically?",
    options: {
      a: "Predicate pushdown",
      b: "Broadcast shuffle",
      c: "Pipeline fusion",
      d: "Schema pruning"
    },
    correct: "a"
  },
  {
    code: "Spark-208",
    question: "What does Tungsten optimize primarily?",
    options: {
      a: "Disk I/O and caching",
      b: "Runtime performance and memory use",
      c: "Network transmission",
      d: "Storage replication"
    },
    correct: "b"
  },
  {
    code: "Spark-209",
    question: "How does whole-stage code generation improve performance?",
    options: {
      a: "Compiles SQL into JVM bytecode",
      b: "Splits queries into smaller partitions",
      c: "Saves logs to cache",
      d: "Forces lazy evaluation"
    },
    correct: "a"
  },
  {
    code: "Spark-210",
    question: "Why should developers study Catalyst and Tungsten internals?",
    options: {
      a: "To redesign Spark architecture",
      b: "To optimize queries and interpret plans better",
      c: "To modify the Spark kernel",
      d: "To install new connectors"
    },
    correct: "b"
  },

  {
    code: "Spark-211",
    question: "What does data partitioning primarily achieve in Spark SQL?",
    options: {
      a: "Reduces storage size",
      b: "Distributes data evenly across nodes",
      c: "Improves schema validation",
      d: "Increases replication factor"
    },
    correct: "b"
  },
  {
    code: "Spark-212",
    question: "Which command demonstrates creating a partitioned table?",
    options: {
      a: "CREATE TABLE USING partition",
      b: "CREATE TABLE PARTITIONED BY (region)",
      c: "CREATE PARTITION TABLE",
      d: "SPLIT TABLE BY region"
    },
    correct: "b"
  },
  {
    code: "Spark-213",
    question: "What is the purpose of bucketing?",
    options: {
      a: "Improve join performance using hash-based grouping",
      b: "Create compressed data",
      c: "Store metadata",
      d: "Simplify shuffles"
    },
    correct: "a"
  },
  {
    code: "Spark-214",
    question: "What does a broadcast join do?",
    options: {
      a: "Moves large tables to all nodes",
      b: "Replicates small tables to avoid shuffle",
      c: "Skips partitioning",
      d: "Executes joins in memory only"
    },
    correct: "b"
  },
  {
    code: "Spark-215",
    question: "What does Adaptive Query Execution (AQE) do?",
    options: {
      a: "Statically partitions data before running",
      b: "Dynamically adjusts join strategies at runtime",
      c: "Rewrites codegen stages",
      d: "Controls Spark event logs"
    },
    correct: "b"
  },

  {
    code: "Spark-216",
    question: "Why do companies migrate from Hive or Teradata to Spark SQL?",
    options: {
      a: "To reduce data accuracy",
      b: "To achieve scalability and unified analytics",
      c: "To use more manual ETL",
      d: "To replace SQL syntax"
    },
    correct: "b"
  },
  {
    code: "Spark-217",
    question: "What is the first step in a typical Spark SQL migration?",
    options: {
      a: "Data cleaning",
      b: "Schema transfer",
      c: "Stream processing",
      d: "Model training"
    },
    correct: "b"
  },
  {
    code: "Spark-218",
    question: "Which command copies data from Hive to Spark SQL?",
    options: {
      a: "IMPORT FROM hive_table",
      b: "CREATE TABLE spark_table AS SELECT * FROM hive_table",
      c: "ATTACH hive_table TO spark_table",
      d: "CONNECT HIVE spark_table"
    },
    correct: "b"
  },
  {
    code: "Spark-219",
    question: "What is a key modernization advantage in Spark?",
    options: {
      a: "Splitting batch and stream pipelines",
      b: "Unifying ETL, batch, and streaming",
      c: "Manual job scheduling",
      d: "Removing caching features"
    },
    correct: "b"
  },
  {
    code: "Spark-220",
    question: "Why is modernization more than performance improvement?",
    options: {
      a: "It enhances infrastructure agility and governance",
      b: "It removes metadata tracking",
      c: "It limits query complexity",
      d: "It replaces SQL with Python"
    },
    correct: "a"
  },

  {
    code: "Spark-221",
    question: "What is the first step in building the Spark SQL pipeline?",
    options: {
      a: "Visualization",
      b: "Data ingestion from multiple sources",
      c: "Query optimization",
      d: "Model training"
    },
    correct: "b"
  },
  {
    code: "Spark-222",
    question: "Which command creates an external table for raw CSV data?",
    options: {
      a: "IMPORT CSV",
      b: "CREATE TABLE USING csv OPTIONS(path …)",
      c: "LOAD FILE INTO TABLE",
      d: "REGISTER EXTERNAL FILE"
    },
    correct: "b"
  },
  {
    code: "Spark-223",
    question: "After data cleaning, where are results stored for reliability?",
    options: {
      a: "Hive table",
      b: "Delta table",
      c: "Local disk",
      d: "In-memory cache"
    },
    correct: "b"
  },
  {
    code: "Spark-224",
    question: "What analytical technique is used to find top-performing regions?",
    options: {
      a: "Caching",
      b: "Window functions",
      c: "File partitioning",
      d: "UDF serialization"
    },
    correct: "b"
  },
  {
    code: "Spark-225",
    question: "What is the overall goal of the capstone project?",
    options: {
      a: "To test Spark UI",
      b: "To demonstrate a full data lifecycle from ingestion to analytics",
      c: "To install cluster components",
      d: "To benchmark storage speed"
    },
    correct: "b"
  }

];

function renderAllQuestions() {
  const container = $("#quiz-container");
  container.empty();

  questions.forEach(q => {
    let html = `
      <div class="question" data-code="${q.code}" data-correct="${q.correct}">
        <p><b>${q.code}</b>: ${q.question}</p>

        <button class="option" data-answer="a">a) ${q.options.a}</button>
        <button class="option" data-answer="b">b) ${q.options.b}</button>
        <button class="option" data-answer="c">c) ${q.options.c}</button>
        <button class="option" data-answer="d">d) ${q.options.d}</button>

        <div class="answer-display"></div>
      </div>
    `;
    container.append(html);
  });
}




$(document).on("click", ".option", function () {

  const btn = $(this);
  const qDiv = btn.closest(".question");

  const clicked = btn.data("answer");
  const correct = qDiv.data("correct");
  const code = qDiv.data("code");

  qDiv.find(".option").addClass("disabled");

  
  let scoreText = "";
  if(clicked===correct){
    scoreText = `(${code}, +20)`;
  }
  else{
    const correctOptionText = qDiv.find(`[data-answer='${correct}']`).text();
    scoreText = `(${code}, -5) | Correct answer: ${correctOptionText}`;
  }

  qDiv.find(".answer-display").text(scoreText);
});


$(document).ready(function () {
  renderAllQuestions();
});
