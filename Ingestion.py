from sqlalchemy import create_engine,text
import pandas as pd
import glob
from urllib.parse import quote
import os
import uuid
import configparser

# First class is for CSV ingestion of NAEB data

class CSVIngestion:

    def __init__(self, db_name, db_user, db_host, db_password, data_path):
        self.db_name = db_name
        self.db_user = db_user
        self.db_host = db_host
        self.db_password = db_password
        self.data_path = data_path
        self.engine = self.create_engine()
    # Create Engine
    def create_engine(self):
        db_password_encoded = quote(self.db_password)
        return create_engine(f'postgresql://{self.db_user}:{db_password_encoded}@{self.db_host}/{self.db_name}')
    
    def execute_query(self, query):
        try:
            with self.engine.begin() as connection:  # Automatically commits or rolls back
                connection.execute(text(query))
        except Exception as e:
            print(f"Error executing query: {e}")
       
   
    # Ingest data into PostgreSQL
    def ingest_data(self):
        # Get a list of all CSV files in the current directory
        if os.path.exists(self.data_path):

            csv_files = glob.glob(self.data_path + '*.csv')
            for file in csv_files:
                if file.endswith('.csv'):
                    self.ingest_csv(file)
        # setting foreign key
        self.add_foreign_key()
        # Closing Connection
        self.close_connection() 
                

    # Read the CSV file into a pandas DataFrame
    def ingest_csv(self, file):
        # Extract the table name from the CSV file name in lower case
        table_name = os.path.splitext(os.path.basename(file))[0].lower()
        df = pd.read_csv(file)
        # print(df.head(20))
        self.transform_data(df)
        # print(df.head(20))
        try:
            #Ingest the DataFrame into the database
            df.to_sql(table_name, con = self.engine, if_exists ='replace', index = False)
            print(f'{file} ingested into {table_name} successfully')
            self.add_primary_key(table_name, "id")
        except Exception as e:
            print(f'Error ingesting {file} into {table_name}: {e}')

        self.close_connection()
       
    def transform_data(self, df):
        # Handling duplicate values
        df.drop_duplicates(keep='first', inplace=True)
        ## 1 Transformation: Deleting columns with all null values
        df.dropna(axis=1, how='all', inplace=True)
    
      
        for column in df.columns:
        # Check if the column is of object type (typically string in pandas)
            if df[column].dtype == 'object':
            # Replace null values with 'unknown' only in this column
                df[column].fillna('unknown', inplace=True)
            
            elif df[column].dtype == 'int64'or df[column].dtype == 'float64':
                df[column].fillna(0, inplace=True)

        return df 
    
     #Setting the Primary key for the tables
    def add_primary_key(self, table_name, column_name):
        try:
            # First, ensure the column contains only unique values and no NULLs
            # Then, alter the table to add the primary key constraint
            query = f'ALTER TABLE {table_name} ADD CONSTRAINT {table_name}_{column_name}_pk PRIMARY KEY ({column_name});'
            self.execute_query(query)
            print(f'Primary key constraint added to {column_name} in {table_name} successfully.')
        except Exception as e:
            print(f'Error adding primary key constraint to {column_name} in {table_name}: {e}')
     
    
    def add_foreign_key(self):
        # to resolve the conflict of use_subcategory
        with self.engine.begin() as connection:
                connection.execute(text("ALTER TABLE uses ALTER COLUMN use_subcategory TYPE INTEGER USING use_subcategory::INTEGER"))
                connection.execute(text("INSERT INTO use_subcategories (id) VALUES (0) ON CONFLICT (id) DO NOTHING;"))
                connection.execute(text("UPDATE uses SET use_subcategory = (SELECT id FROM use_subcategories LIMIT 1) WHERE use_subcategory NOT IN (SELECT id FROM use_subcategories);"))
        # Adding foreign key constraints
        foreign_key  = [
            "ALTER TABLE USES ADD CONSTRAINT USES_USE_CATEGORIES_FK FOREIGN KEY (use_category) REFERENCES   use_categories(id);",
            "ALTER TABLE USES ADD CONSTRAINT USES_USE_SUBCATEGORIES_FK FOREIGN KEY (use_subcategory) REFERENCES use_subcategories(id);",
            "ALTER TABLE USES ADD CONSTRAINT USES_TRIBE_FK FOREIGN KEY (tribe) REFERENCES tribes(id);",
            "ALTER TABLE USES ADD CONSTRAINT USES_SPECIES_FK FOREIGN KEY (species) REFERENCES species(id);",
            "ALTER TABLE USES ADD CONSTRAINT USES_SOURCE_FK FOREIGN KEY (source) REFERENCES sources(id);",

        ]
        for fk in foreign_key:

            try:            
                self.execute_query(fk)
                print(f'Foreign key constraint added successfully.')

            except Exception as e:
                print(f'Error adding foreign key constraint: {e}')

  
    def close_connection(self):
        return self.engine.dispose()
    
#Second class is for TXT ingestion of NPASS data
        
class TXTIngestion:

    def __init__(self, db_name, db_user, db_host, db_password, data_path):
        self.db_name = db_name
        self.db_user = db_user
        self.db_host = db_host
        self.db_password = db_password
        self.data_path = data_path
        self.engine = self.create_engine()

    # Create Engine
    def create_engine(self):
        db_password_encoded = quote(self.db_password)
        return create_engine(f'postgresql://{self.db_user}:{db_password_encoded}@{self.db_host}/{self.db_name}')
    
    # Ingesting Data into PostgreSQL
    def ingest_data(self):

        if os.path.exists(self.data_path):
            txt_files = glob.glob(self.data_path + '*.txt')
            for file in txt_files:
                if file.endswith('.txt'):
                    self.ingest_txt(file)
    
        self.close_connection()
    ## Execute Query
    def execute_query(self, query):
        with self.engine.connect() as connect:
            connect.execute(text(query))
            connect.commit()
    
    # Ingest Data
    def ingest_txt(self, file):
       
        df = pd.read_table(file, low_memory=False, na_values=['n.a.'])
        # Extracting the last name from the file name
        table = os.path.splitext(os.path.basename(file))[0].lower()
        table_parts = table.split('_')
        table_name = ''.join(table_parts[3:])
        print("Table Name: ", table_name)
        # print(df.head())
        self.transform_txt_data(df)
        # print('After Transformation: ', df.head())
        try:
            df.to_sql(table_name, con=self.engine, if_exists='replace', index=False)
            # Adding primary key
            if ('np_id' not in df.columns) or (not df['np_id'].is_unique) or df['np_id'].isnull().any():
                self.add_primary_key(table_name, 'custom_id')
            else:
                self.add_primary_key(table_name, 'np_id')

            print(f'{file} ingested into {table_name} successfully')
           
        except Exception as e:
            print(f'Error ingesting {file} into {table}: {e}')
    
    # Transforming Data
    def transform_txt_data(self, df):
       
         # Handling duplicate values 
        df.drop_duplicates(keep = 'first', inplace=True)
        ## Deleting columns with all null values
        df.dropna(axis=1, how='all', inplace=True)
        ## Replacing null values and converting to lower case for data consistency
        for column in df.columns:
            if df[column].dtype == 'object':
                df[column].fillna('unknown', inplace=True)
                df[column] = df[column].apply(lambda x: x.lower())
            elif df[column].dtype == 'int64' or df[column].dtype == 'float64':
                df[column].fillna(0, inplace=True)
        # Adding a custom id column if np_id is not present
        if ('np_id' not in df.columns) or (not df['np_id'].is_unique) or df['np_id'].isnull().any():
            df['custom_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    
        return df
    
    def add_primary_key(self, table_name, column_name):
        try:
            # First, ensure the column contains only unique values and no NULLs
            # Then, alter the table to add the primary key constraint
            query = f'ALTER TABLE {table_name} ADD CONSTRAINT {table_name}_{column_name}_pk PRIMARY KEY ({column_name});'
            self.execute_query(query)
            print(f'Primary key constraint added to {column_name} in {table_name} successfully.')
        except Exception as e:
            print(f'Error adding primary key constraint to {column_name} in {table_name}: {e}')
   
        
    def close_connection(self):
        self.engine.dispose()
   
# Main Function
if __name__ == '__main__':

 
    # Database connection details
    config = configparser.ConfigParser()
    config.read_file(open('credentials.config'))
    db_name = config['DB']['db_name']
    db_user = config['DB']['db_user']
    db_host = config['DB']['db_host']
    db_password = config['DB']['db_password']
    db_password_encoded = quote(db_password)
    data_path = 'C:\\Users\\USER\\Desktop\\training\\Case_Study\\naeb\\data\\naeb_dump\\'
    data_path_txt = 'C:\\Users\\USER\\Desktop\\training\\Case_Study\\naeb\\data\\NPASS\\'

   # Create the database engine
    db_ingestion = CSVIngestion(db_name, db_user, db_host, db_password, data_path)
    db_ingestion.ingest_data()
    db_ingestion.close_connection()

    # #Create the database engine for txt
    db_ingestion_txt = TXTIngestion(db_name, db_user, db_host, db_password, data_path_txt)
    db_ingestion_txt.ingest_data()
    db_ingestion_txt.close_connection()

