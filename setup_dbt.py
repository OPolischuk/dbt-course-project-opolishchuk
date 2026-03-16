import os
from pathlib import Path

# Template for the dbt profile
# Replace placeholders with your actual Snowflake credentials
DBT_PROFILE_TEMPLATE = """
dbt_course_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <YOUR_SNOWFLAKE_ACCOUNT_HERE>
      user: <YOUR_USERNAME_HERE>
      password: <YOUR_PASSWORD_HERE>
      role: <YOUR_ACCOUNT_NAME>
      warehouse: <YOUR_WAREHOUSE_NAME>
      database: <YOUR_DATABASE_NAME>
      schema: <YOUR_SCHEMA_NAME>
      threads: 10
      client_session_keep_alive: False
"""

def sync_dbt_profile():
    """
    Ensures that the dbt profile directory and configuration exist.
    Appends the project profile if it's missing from profiles.yml.
    """
    home_dbt = Path.home() / ".dbt"
    profile_path = home_dbt / "profiles.yml"

    # Ensure the .dbt directory exists in the home folder
    if not home_dbt.exists():
        home_dbt.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {home_dbt}")

    # Create the file if it doesn't exist, otherwise check for the profile
    if not profile_path.exists():
        with open(profile_path, "w", encoding="utf-8") as f:
            f.write(DBT_PROFILE_TEMPLATE.strip())
        print(f"New profiles.yml created at {profile_path}")
    else:
        with open(profile_path, "r", encoding="utf-8") as f:
            existing_config = f.read()

        # Avoid duplicate profile entries
        if "dbt_course_project:" in existing_config:
            print("Profile 'dbt_course_project' already exists. Skip.")
        else:
            with open(profile_path, "a", encoding="utf-8") as f:
                f.write("\n\n" + DBT_PROFILE_TEMPLATE.strip())
            print("Successfully added 'dbt_course_project' to profiles.yml")

if __name__ == "__main__":
    try:
        print("Starting dbt environment setup...")
        sync_dbt_profile()
        print("Setup finished successfully.")
    except Exception as e:
        print(f"Error during setup: {e}")
