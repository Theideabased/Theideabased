# first i will try to import the crypto currency data using big query 

# In[3]:


from google.cloud import bigquery
import os
import pandas
credentials_path ='c:/regal-dynamo-331703-7ed7e214c7c0.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()
# Construct a reference to the "crypto_bitcoin" dataset
dataset_ref = client.dataset("crypto_bitcoin", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Get a list of available tables 
tables = list(client.list_tables(dataset)) # Your code here
list_of_tables = [table.table_id for table in tables]
# Print your answer
print(list_of_tables)




# In[5]:


# Construct a reference to the "transactions" table
transactions_table_ref = dataset_ref.table("transactions")

# API request - fetch the table
transactions_table = client.get_table(transactions_table_ref)

# Preview the first five lines of the "transactions" table
client.list_rows(transactions_table, max_results=10).to_dataframe()


# In[4]:


# Construct a reference to the "blocks" table
blocks_table_ref = dataset_ref.table("blocks")

# API request - fetch the table
blocks_table = client.get_table(blocks_table_ref)

# Preview the first ten lines of the "transactions" table
client.list_rows(blocks_table, max_results=10).to_dataframe()


# ## Now i want analyze transaction  ##
# First of all to analyze the transation in crypto_bitcoin i will get the the number of transaction that occur in all years   

# In[14]:


#This is my query code
no_of_transactions_query = """
                           SELECT EXTRACT (YEAR from block_timestamp_month) AS years, COUNT(1) AS num_trans
                           FROM `bigquery-public-data.crypto_bitcoin.transactions`
                           GROUP BY years
                           ORDER BY years 
                           """
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
num_of_trans_query_job = client.query(no_of_transactions_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
num_of_trans= num_of_trans_query_job.to_dataframe()

# View the rows of results
print(num_of_trans)


# In[15]:


from matplotlib import pyplot as plt
num_of_trans.plot(x = 'years', y = 'num_trans', kind = 'bar')
plt.title('number of people that transact bitcoin')
plt.xlabel('year')
plt.ylabel('number of people')
plt.show()

