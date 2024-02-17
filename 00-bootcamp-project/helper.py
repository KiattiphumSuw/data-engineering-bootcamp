import csv

def get_by_postgres(table, header, data_path, output, cursor):
    with open(f"{data_path}/{output}.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        query = f"select * from {table}"
        cursor.execute(query)

        results = cursor.fetchall()
        for each in results:
            writer.writerow(each)