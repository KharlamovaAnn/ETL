import csv
import random
from datetime import datetime, timedelta
import xlsxwriter

def generate_postgres_sql(filename, num_rows):
    print(f"Generating {filename}...")
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("CREATE TABLE IF NOT EXISTS call_logs (\n")
        f.write("    call_id SERIAL PRIMARY KEY,\n")
        f.write("    operator_id INT,\n")
        f.write("    call_date TIMESTAMP,\n")
        f.write("    duration_sec INT\n")
        f.write(");\n\n")
        f.write("TRUNCATE TABLE call_logs;\n\n")
        
        batch_size = 10000
        start_date = datetime(2025, 1, 1)
        
        for i in range(0, num_rows, batch_size):
            values = []
            for j in range(batch_size):
                curr_id = i + j + 1
                if curr_id > num_rows: break
                
                op_id = random.randint(1, 1000)
                date = start_date + timedelta(seconds=random.randint(0, 31536000))
                duration = random.randint(10, 600)
                values.append(f"({op_id}, '{date.isoformat()}', {duration})")
            
            f.write("INSERT INTO call_logs (operator_id, call_date, duration_sec) VALUES\n")
            f.write(",\n".join(values) + ";\n\n")
    print(f"PostgreSQL data generated.")

def generate_csv_topics(filename, num_rows):
    print(f"Generating {filename}...")
    topics = ["Sales", "Technical Support", "Billing", "Complaints", "General Information", "Refunds", "Account Recovery"]
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["call_id", "topic_name"])
        for i in range(1, num_rows + 1):
            writer.writerow([i, random.choice(topics)])
    print(f"CSV data generated.")

def generate_excel_kpi(filename, num_rows):
    print(f"Generating {filename}...")
    workbook = xlsxwriter.Workbook(filename)
    worksheet = workbook.add_worksheet()
    
    headers = ["operator_id", "operator_name", "kpi_score"]
    for col, header in enumerate(headers):
        worksheet.write(0, col, header)
    
    for i in range(1, num_rows + 1):
        worksheet.write(i, 0, i)
        worksheet.write(i, 1, f"Operator_{i}")
        worksheet.write(i, 2, round(random.uniform(1.0, 5.0), 2))
    
    workbook.close()
    print(f"Excel data generated.")

if __name__ == "__main__":
    generate_postgres_sql("call_logs.sql", 1000000)
    generate_excel_kpi("operator_kpi.xlsx", 100000)
    generate_csv_topics("call_topics.csv", 100000)
