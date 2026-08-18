# Databricks notebook source
# MAGIC %md
# MAGIC ## Logging in to Jira and Creating an API Token
# MAGIC
# MAGIC To use Jira APIs, you need to authenticate with an API token. Follow these steps:
# MAGIC
# MAGIC ### 1. Log in to Jira
# MAGIC - Go to [Jira Login](https://id.atlassian.com/login).
# MAGIC - Enter your Atlassian account email and password.
# MAGIC
# MAGIC ### 2. Create an API Token
# MAGIC - Visit [API Token Management](https://id.atlassian.com/manage-profile/security/api-tokens).
# MAGIC - Click **Create API token**.
# MAGIC - Enter a label for your token and click **Create**.
# MAGIC - Copy the generated token and store it securely.
# MAGIC
# MAGIC ### 3. Use the API Token
# MAGIC - Use your email and API token for authentication in scripts or integrations.
# MAGIC - Example:  
# MAGIC   
# MAGIC   Username: your-email@example.com
# MAGIC   API Token: <your-api-token>
# MAGIC   
# MAGIC
# MAGIC > **Note:** Keep your API token confidential and do not share it.

# COMMAND ----------

# DBTITLE 1,Cell 1
from databricks.sdk import WorkspaceClient

# Initialize Databricks workspace client (uses notebook authentication)
w = WorkspaceClient()

# Create a secret scope named 'jira-scope'
try:
    w.secrets.create_scope(scope="jira-scope")
    print("Secret scope 'jira-scope' created successfully")
except Exception as e:
    if "already exists" in str(e).lower():
        print("Secret scope 'jira-scope' already exists")
    else:
        raise

# Store the Jira email in the secret scope
w.secrets.put_secret(
    scope="jira-scope",
    key="jira-email",
    string_value="tgrappstech@gmail.com"
)
print("Stored jira-email secret")

# Store the Jira API token in the secret scope
w.secrets.put_secret(
    scope="jira-scope",
    key="jira-api-token",
    string_value=" - =C2AE194E"
)
print("Stored jira-api-token secret")

# Retrieve the secrets for use
JIRA_EMAIL = dbutils.secrets.get(scope="jira-scope", key="jira-email")
JIRA_API_TOKEN = dbutils.secrets.get(scope="jira-scope", key="jira-api-token")

print("\nCredentials retrieved successfully and ready to use")

# COMMAND ----------

