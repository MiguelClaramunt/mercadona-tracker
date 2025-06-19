import sqlite3
import json
import ast

# Connect to the database
conn = sqlite3.connect("/home/miguel/git/mercadona-tracker/src/mercadona.db")
cursor = conn.cursor()

# Fetch all rows from the 'dumps' table
cursor.execute("SELECT rowid, content FROM dumps")
rows = cursor.fetchall()

# Process and update each row
for id, content in rows:
    try:
        # Safely parse the string to a Python dictionary
        data_dict = ast.literal_eval(content)
        # Convert the dictionary to a JSON string without spaces
        json_content = json.dumps(data_dict, separators=(',', ':'), ensure_ascii=False)
        # Update the content in the database
        cursor.execute("UPDATE dumps SET content = ? WHERE rowid = ?", (json_content, id))
    except (ValueError, SyntaxError):
        print(f"Error parsing content for rowid {id}, skipping.")

# Commit changes and close the connection
conn.commit()
conn.close()