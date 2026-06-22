import subprocess
import os
import sys
import time

def run_command(cmd):
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}", file=sys.stderr)
        print(f"Output: {e.stdout}", file=sys.stderr)
        return False

def main():
    print("AIRFLOW INITIALIZATION")
    
    # Run db init to create database schema
    print("Initializing database...")
    if not run_command("airflow db init"):
        sys.exit(1)
    
    # Create admin user
    print("Creating admin user...")
    airflow_admin_username = os.getenv("AIRFLOW_ADMIN_USERNAME", "admin")
    airflow_admin_firstname = os.getenv("AIRFLOW_ADMIN_FIRSTNAME", "Admin")
    airflow_admin_lastname = os.getenv("AIRFLOW_ADMIN_LASTNAME", "User")
    airflow_admin_email = os.getenv("AIRFLOW_ADMIN_EMAIL", "admin@example.com")
    airflow_admin_password = os.getenv("AIRFLOW_ADMIN_PASSWORD", "password")
    create_user_cmd = (
        "airflow users create "
        f"--username {airflow_admin_username} "
        f"--firstname {airflow_admin_firstname} "
        f"--lastname {airflow_admin_lastname} "
        "--role Admin "
        f"--email {airflow_admin_email} "
        f"--password {airflow_admin_password}"
    )
    
    result = subprocess.run(create_user_cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0 and "already exists" not in result.stdout and "already exists" not in result.stderr:
        print(f"Warning: {result.stderr}")
    
    print("AIRFLOW INITIALIZATION COMPLETED!")

if __name__ == "__main__":
    main()
