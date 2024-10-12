import os
import pandas as pd
import pyodbc
import shutil
from datetime import datetime

# Step 1: Define folder paths
#source_folder = 'C:\\Users\\Administrator\\Downloads\\List\\X\\Active'

#current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
#processed_folder = f'C:\\Users\\Administrator\\Downloads\\Processed_{current_datetime}'



# Ensure the processed folder exists
#if not os.path.exists(processed_folder):
#    os.makedirs(processed_folder)

# Step 2: Define the connection to SQL Server (reuse the connection)
conn = pyodbc.connect(
    'DRIVER=ODBC Driver 17 for SQL Server;'
    'SERVER=HOME;'
    'DATABASE=SaucesDb;'
    'Trusted_Connection=yes;'
)
cursor = conn.cursor()

def get_source_folders():
    query = "SELECT SourceFolderPath, Type FROM SourceFolders where IsActive = 1"
    cursor.execute(query)
    return cursor.fetchall()


# Function to map pandas data types to SQL Server data types
def map_dtype_to_sql(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BIT'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'VARCHAR(255)'



# Function to get columns from a template table
def get_template_columns(template_table_name):
    query = f"SELECT Columns FROM SaucesDb.dbo.{template_table_name} WHERE status = 'Use'"
    #print(query)
    cursor.execute(query)
    return {row[0] for row in cursor.fetchall()}

# Function to add new columns to the SQL table (if any new columns are required)
def add_new_columns_to_table(table_name, new_columns):
    for col in new_columns:
        sql_type = map_dtype_to_sql(pd.Series(dtype='object'))  # Default to VARCHAR for new columns
        alter_table_query = f"ALTER TABLE {table_name} ADD [{col}] {sql_type};"
        try:
            cursor.execute(alter_table_query)
            print(f"Added column '{col}' to table '{table_name}'.")
        except Exception as e:
            print(f"Error adding column '{col}' to table '{table_name}': {str(e)}")



# Special keywords for table name logic
special_keywords = ['Probate', 'Tax', 'Eviction']

def log_insertion_error(file_name, sheet_name, row_index, error_message):
    error_insert_query = '''
        INSERT INTO dbo.Insertion_Errors (File_Name, Sheet_Name, Row_Index, Error_Message)
        VALUES (?, ?, ?, ?)
    '''
    cursor.execute(error_insert_query, file_name, sheet_name, row_index, error_message)
    conn.commit()

# Function to check if a table exists
def table_exists(table_name):
    check_table_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
    cursor.execute(check_table_query)
    return cursor.fetchone()[0] > 0

# Function to get current columns in the SQL table
def get_current_columns(table_name):
    query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    cursor.execute(query)
    return {row[0] for row in cursor.fetchall()}

# Function to add new columns to the SQL table
def add_new_columns_to_table(table_name, new_columns):
    for col in new_columns:
        sql_type = map_dtype_to_sql(pd.Series(dtype='object'))  # Default to VARCHAR for new columns
        alter_table_query = f"ALTER TABLE {table_name} ADD [{col}] {sql_type};"
        try:
            cursor.execute(alter_table_query)
            print(f"Added column '{col}' to table '{table_name}'.")
        except Exception as e:
            print(f"Error adding column '{col}' to table '{table_name}': {str(e)}")

# Batch insert function to reduce DB operations
def batch_insert_to_sql(df, insert_query, table_name):
    batch_size = 1000  # Number of rows to insert at a time
    total_rows = len(df)
    for start in range(0, total_rows, batch_size):
        batch_df = df[start:start + batch_size]
        try:
            cursor.executemany(insert_query, batch_df.values.tolist())
        except Exception as e:
            log_insertion_error(table_name, file_name, start, str(e))
    conn.commit()


# Special keywords mapped to corresponding template tables
keyword_to_template_table = {
    'Sauce3': 'Sauce3Template',
    'Sauce1': 'Sauce1Template',
    'Sauce2': 'Sauce2Template',
    # Add other mappings here if necessary
}

# Function to get the template table based on the file name
def get_template_table_name(file_type):
    for keyword, template_table in keyword_to_template_table.items():
        if keyword.lower() in file_type.lower():
            return template_table
    return None  # Return None if no keyword matches




# Function to manually chunk large DataFrames
def chunk_df(df, chunk_size=1000):
    for start in range(0, df.shape[0], chunk_size):
        yield df.iloc[start:start + chunk_size]

def get_table_name(file_name, sheet_name):
    # Look for special keywords in the file name
    for keyword in special_keywords:
        if keyword.lower() in file_name.lower():
            table_name = f'{keyword}'.replace(" ", "_")
            #print(f"Special keyword '{keyword}' found in file name. Table name set to: {table_name}")
            return table_name
    # Default table name if no keyword found
    #table_name = f'{file_name.replace(".xlsx", "")}_{sheet_name}'.replace(" ", "_")
    #print(f"No special keyword found. Default table name: {table_name}")
    return table_name

table_name = 'null'

def makeProcessedFolder(foldername):
    current_datetime = datetime.now().strftime('%Y-%m-%d_%H-')
    processed_folder = f'C:\\log\\Processed_{current_datetime}_{foldername}'
    #print('Making Folder' + processed_folder )
    if not os.path.exists(processed_folder):
        os.makedirs(processed_folder,exist_ok=False)
    return processed_folder



processed_files_log = set()

def setProcessedFile():
    processed_files_log = set()
# Before processing, load already processed files from a log table (or file)
    cursor.execute("SELECT File_Name FROM ProcessedFilesLog")
    for row in cursor.fetchall():
        processed_files_log.add(row[0])



# Process each file based on folder and type from SQL
for source_folder, file_type in get_source_folders():
    print(f"Outer Loop in folder: {source_folder} of type: {file_type}")

    # Step 2: Create processed folder with timestamp
    # Step 3: Traverse through all subfolders and files in the source folder
    for dirpath, _, filenames in os.walk(source_folder):
        for file_name in filenames:
            if file_name.endswith('.xlsx') and file_name not in processed_files_log:  # Process only Excel files
                print(f"Inner Loop file: {file_name}")

                # Step 4: Full file path (including subfolders)
                file_path = os.path.join(dirpath, file_name)

                # Step 5: Determine template table based on the file name
                template_table_name = get_template_table_name(file_type)
                if not template_table_name:
                    print(f"No matching keyword found in the file name '{file_name}'. Skipping file.")
                    continue

                #print(f"Using template table: {template_table_name}")

                # Step 6: Get the columns from the identified template table
                template_columns = get_template_columns(template_table_name)
                
                
                # Step 7: Open and process the Excel file
                try:
                    sheets_dict = pd.read_excel(file_path, sheet_name=None, engine='openpyxl')
                    for sheet_name, df in sheets_dict.items():
                        #print(f"Processing sheet: {sheet_name}")

                        # Generate table name based on file name and sheet name
                        table_name = file_type #get_table_name(file_name, sheet_name)


                        #print(table_name)

                        df.columns = [col.replace(' - ', ' ').replace(' ', '_').replace('$', '')
                                    .replace('-', '_').replace('.', '').replace('#', 'No')
                                    .replace('Use', 'Uses').replace('(', '').replace(')', '')
                                    for col in df.columns]
                        df = df.replace({float('nan'): None})  # Replace NaN values

                        # Skip empty sheets
                        if df.empty:
                            print(f"Sheet '{sheet_name}' is empty. Skipping.")
                            continue

                        # Capture current datetime for bulk insert
                        bulk_insert_datetime = datetime.now()

                        # Add additional columns for metadata (File Name, Sheet Name, Insertion Datetime)
                        df['File_Name'] = file_name
                        df['Sheet_Name'] = sheet_name
                        df['Bulk_Insert_DateTime'] = bulk_insert_datetime
                        df['IsShifted'] = 0

                        # Filter DataFrame to keep only columns that exist in the template table
                        filtered_columns = [col for col in df.columns if col in template_columns]
                        df = df[filtered_columns]

                        # Skip if no columns match the template table
                        if df.empty:
                            print(f"No matching columns in sheet '{sheet_name}'. Skipping.")
                            continue

                        # Step 6: Check if the table already exists
                        if not table_exists(table_name):
                            print(f"Creating new table: {table_name}")

                            # Dynamically generate SQL table creation statement
                            sql_columns = []
                            for col in df.columns:
                                sql_type = map_dtype_to_sql(df[col].dtype)
                                sql_columns.append(f'[{col}] {sql_type}')

                            create_table_statement = f'''
                            CREATE TABLE {table_name} (
                                {', '.join(sql_columns)}
                            );
                            '''
                            cursor.execute(create_table_statement)
                        else:
                            # Get current columns in the SQL table
                            existing_columns = get_current_columns(table_name)
                            # Identify new columns in the DataFrame
                            new_columns = set(df.columns) - existing_columns
                            # Add new columns to the SQL table if they do not exist
                            if new_columns:
                                #print(f"NEW COLUMNS FOUND'{sheet_name}'")
                                add_new_columns_to_table(table_name, new_columns)

                        # Prepare SQL insert query

                        placeholders = ', '.join('?' * len(df.columns))
                        columns_str = ', '.join([f'[{col}]' for col in df.columns])

                        insert_query = f'''
                            INSERT INTO {table_name} ({columns_str})
                            VALUES ({placeholders})
                        '''

                # Step 7: Insert data in chunks
                    for chunk in chunk_df(df):
                        batch_insert_to_sql(chunk, insert_query, table_name)

                
                # Step 8: Move the processed file to the "Processed" folder
                    processed_folder = makeProcessedFolder(table_name)
                    processed_subfolder = os.path.join(processed_folder, os.path.relpath(dirpath, source_folder))
                    if not os.path.exists(processed_subfolder):
                        os.makedirs(processed_subfolder)

                    shutil.move(file_path, os.path.join(processed_subfolder, file_name))
                    print(f"File '{file_name}' processed and moved to '{processed_subfolder}'.")

                    cursor.execute("INSERT INTO ProcessedFilesLog (File_Name, File_Path) VALUES (?,?)", (file_name,file_path))
                    setProcessedFile()
                    
                except Exception as e:
                    print(f"Error processing file {file_name}: {str(e)}")
                    log_insertion_error(table_name, file_name, '0', str(e))
                    continue

        

# Step 9: Close the database connection
cursor.close()
conn.close()
