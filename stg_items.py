#CLEANIN OF DATA

escape_characters=['\n', '\t', '\r', '\\', '\b','\f', '\v','\"',"\'",'\0','\a','\x1b', '""']

def replace_excape_characters(line, esc_char):
    for char in esc_char:
        line=line.replace(char, '?')
        return line
    

orginal_file= r"C:\Users\BIRSTSA\Desktop\stg_items.txt"
cleaned_file = r"C:\Users\BIRSTSA\Desktop\cleaned_file.txt"

with open(orginal_file, 'r') as orgfile, open(cleaned_file, 'w', newline='') as cleanfile:
    for line in orgfile:
        clean_line = replace_excape_characters(line, escape_characters)
        cleanfile.write(clean_line)

print(f"escape characters replaced with ? in  {cleaned_file}")


import csv

original_file_path = r"C:\Users\BIRSTSA\Desktop\stg_items.txt"
cleaned_file_path = r"C:\Users\BIRSTSA\Desktop\cleaned_file.txt"
EXPECTED_COLUMN = 10

with open(original_file_path,'r') as orgfile, open(cleaned_file_path,'w', newline='') as cleanfile:
    reader = csv.reader(orgfile, delimiter ='\t')
    writer = csv.writer(cleanfile, delimiter ='\t')
    for row in reader:
        if len(row)== EXPECTED_COLUMN:
            writer.writerow(row)
        else:
            print(f"skipping problematic row:{row}")


#LOADING OF DATA

import psycopg2
try:
    conn = psycopg2.connect(
        host="10.150.136.36",
        database="businessIntelligenceDB",
        user="postgres",
        password="@Welcome2Post"

    )
    cur = conn.cursor()
    schema_name=' "deviceSchema" '
    table_name="stg_items"

    text_file_path = r"C:\Users\BIRSTSA\Desktop\cleaned_file.txt"

    cur.execute(f"TRUNCATE TABLE {schema_name}.{table_name};")
    conn.commit()

    with open(text_file_path, 'r') as file:
        cur.copy_expert(f"""COPY {schema_name}.{table_name} FROM STDIN WITH CSV HEADER DELIMITER '\t' NULL AS ''""", file)
        conn.commit()

except psycopg2.Error as e:
    print(f"An error occured with DB: {e}")

except FileNotFoundError:
    print(f"Error: The file {text_file_path} not found.")

except Exception as e:
    print(f"An unexpected error: {e}")

finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()

print("SUCCESSFUL")




                                                                                    
    


